package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"log"
	"sort"

	//	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

const (
	CANDIDATE = 1
	FOLLOWER  = 2
	LEADER    = 3
)

const heartBeatInterval = 100 * time.Millisecond

// A Go object implementing a single Raft peer.
type EntryData struct {
	Term    int
	Command any
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []EntryData
	dirty       bool

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state           int
	lastHeartBeat   time.Time
	electionTimeout time.Duration
	persistedLogLen int

	applyCh     chan raftapi.ApplyMsg
	applyCond   *sync.Cond
	persistCond *sync.Cond
	replCond    []*sync.Cond
	lastSent    []time.Time
	inFlight    []bool
	sendSeq     []int

	snapshot      []byte
	snapshotIndex int
	snapshotTerm  int
}

func (rf *Raft) resetElection() {
	// pause for a random amount of time between 50 and 350
	// milliseconds.
	ms := 150 + (rand.Int63() % 300)

	rf.lastHeartBeat = time.Now()
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (3A).
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var votedFor int
	var sLog []EntryData
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&sLog) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {
		log.Fatalf("Could not decode persisted data")
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.log = sLog
	rf.snapshotIndex = snapshotIndex
	rf.snapshotTerm = snapshotTerm
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) stepDownToFollower(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.dirty = true
	}

	rf.state = FOLLOWER
}

func (rf *Raft) updateCommitIndex() {
	matches := make([]int, len(rf.peers))
	copy(matches, rf.matchIndex)
	matches[rf.me] = rf.absIdx(len(rf.log)) - 1

	sort.Ints(matches)

	majorityIndex := matches[len(rf.peers)/2]

	if majorityIndex > rf.commitIndex && majorityIndex > rf.snapshotIndex {
		if rf.log[rf.relIdx(majorityIndex)].Term == rf.currentTerm {
			rf.commitIndex = majorityIndex
			rf.applyCond.Broadcast()
		}
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if rf.snapshotIndex >= args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()
	rf.applyCh <- raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.snapshotIndex >= args.LastIncludedIndex {
		return
	}

	oldRel := args.LastIncludedIndex - rf.snapshotIndex

	if oldRel < len(rf.log) && rf.log[oldRel].Term == args.LastIncludedTerm {
		newLog := make([]EntryData, len(rf.log)-oldRel)
		copy(newLog, rf.log[oldRel:])
		rf.log = newLog
	} else {
		rf.log = []EntryData{{Term: args.LastIncludedTerm}}
	}

	rf.snapshot = args.Data
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	rf.dirty = true
	rf.persistCond.Broadcast()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) relIdx(idx int) int {
	return idx - rf.snapshotIndex
}

func (rf *Raft) absIdx(idx int) int {
	return idx + rf.snapshotIndex
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	relIndex := rf.relIdx(index)
	if relIndex <= 0 || relIndex >= len(rf.log) {
		return
	}

	rf.snapshot = snapshot
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.log[relIndex].Term

	newLog := make([]EntryData, len(rf.log)-relIndex)
	copy(newLog, rf.log[relIndex:])

	rf.log = newLog

	rf.dirty = true
	rf.persistCond.Broadcast()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	//Check if log is up to date
	lastIndex := rf.absIdx(len(rf.log) - 1)
	lastTerm := rf.log[len(rf.log)-1].Term

	uptoDate := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptoDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.dirty = true
		rf.resetElection()
	}

	rf.persistCond.Broadcast()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []EntryData
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictingTerm   int
	ConflictingIndex  int
	ConflictingLogLen int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.ConflictingTerm = -1
	reply.ConflictingIndex = -1
	reply.ConflictingLogLen = -1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm || rf.state == CANDIDATE {
		rf.stepDownToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	rf.resetElection()

	if args.PrevLogIndex < rf.snapshotIndex {
		skip := rf.snapshotIndex - args.PrevLogIndex
		if skip > len(args.Entries) {
			reply.Success = true
			return
		}

		args = &AppendEntriesArgs{
			Term:         args.Term,
			LeaderId:     args.LeaderId,
			PrevLogIndex: rf.snapshotIndex,
			PrevLogTerm:  rf.snapshotTerm,
			Entries:      args.Entries[skip:],
			LeaderCommit: args.LeaderCommit,
		}
	}

	if args.PrevLogIndex >= rf.absIdx(len(rf.log)) {
		//Log is too short
		reply.ConflictingLogLen = rf.absIdx(len(rf.log))
		//rf.persistCond.Broadcast()
		return
	}

	if rf.log[rf.relIdx(args.PrevLogIndex)].Term != args.PrevLogTerm {
		conflictingTerm := rf.log[rf.relIdx(args.PrevLogIndex)].Term

		n := rf.relIdx(args.PrevLogIndex)
		for n > 0 && rf.log[n-1].Term == conflictingTerm {
			n--
		}

		reply.ConflictingTerm = conflictingTerm
		reply.ConflictingIndex = rf.absIdx(n)
		return
	}

	for i, entry := range args.Entries {
		idx := rf.relIdx(args.PrevLogIndex) + i + 1
		if idx < len(rf.log) {
			if rf.log[idx].Term != entry.Term {
				rf.log = append(rf.log[:idx], args.Entries[i:]...)
				rf.dirty = true
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.dirty = true
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		newCommit := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Broadcast()
		}
	}

	reply.Success = true
	rf.persistCond.Broadcast()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) handleAppendEntriesResponse(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(reply.Term)
		rf.resetElection()
		return
	}

	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		if match > rf.matchIndex[server] {
			rf.matchIndex[server] = match
			rf.updateCommitIndex()
		}
		rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)
		return
	}

	if args.PrevLogIndex+1 != rf.nextIndex[server] {
		//Stale
		return
	}

	if reply.ConflictingLogLen != -1 {
		rf.nextIndex[server] = reply.ConflictingLogLen
	} else {
		n := -1
		for idx := len(rf.log) - 1; idx > 0; idx-- {
			if rf.log[idx].Term == reply.ConflictingTerm {
				n = idx
				break
			}
		}

		if n == -1 {
			rf.nextIndex[server] = reply.ConflictingIndex
		} else {
			rf.nextIndex[server] = rf.absIdx(n) + 1
		}
	}

	rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1, 1)
}

func (rf *Raft) shouldSend(server int) bool {
	if rf.state != LEADER {
		return false
	}

	if time.Since(rf.lastSent[server]) >= heartBeatInterval {
		return true
	}

	return rf.nextIndex[server] < rf.absIdx(len(rf.log)) && !rf.inFlight[server]
}

func (rf *Raft) sendAndHandle(server int, args *AppendEntriesArgs, seq int) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if seq == rf.sendSeq[server] {
		rf.inFlight[server] = false
	}

	if !ok || rf.state != LEADER || rf.currentTerm != args.Term {
		return // retry on the next heartbeat; don't spin against a dead peer
	}

	rf.handleAppendEntriesResponse(server, args, reply)
	rf.replCond[server].Broadcast() // send more right away if anything is pending
	rf.persistCond.Broadcast()
}

func (rf *Raft) replicator(server int) {
	rf.mu.Lock()
	for {
		for !rf.shouldSend(server) {
			rf.replCond[server].Wait()
		}

		if rf.nextIndex[server] <= rf.snapshotIndex {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.snapshotIndex,
				LastIncludedTerm:  rf.snapshotTerm,
				Data:              rf.snapshot,
			}

			rf.lastSent[server] = time.Now()
			rf.mu.Unlock()

			reply := &InstallSnapshotReply{}
			ok := rf.sendInstallSnapshot(server, args, reply)

			rf.mu.Lock()
			if ok && rf.state == LEADER && rf.currentTerm == args.Term {
				if reply.Term > rf.currentTerm {
					rf.stepDownToFollower(reply.Term)
					rf.resetElection()
					rf.persistCond.Broadcast()
				} else {
					rf.nextIndex[server] = max(rf.nextIndex[server], rf.snapshotIndex+1)
					rf.matchIndex[server] = max(rf.matchIndex[server], rf.snapshotIndex)
				}
			}
			continue
		}

		prev := rf.nextIndex[server] - 1
		var entries []EntryData
		if !rf.inFlight[server] {
			entries = append([]EntryData(nil), rf.log[rf.relIdx(prev)+1:]...)
		}

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prev,
			PrevLogTerm:  rf.log[rf.relIdx(prev)].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.lastSent[server] = time.Now()
		rf.inFlight[server] = true
		rf.sendSeq[server]++
		go rf.sendAndHandle(server, args, rf.sendSeq[server])
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	for {
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		lo := rf.lastApplied + 1
		if lo <= rf.snapshotIndex {
			lo = rf.snapshotIndex + 1
		}

		hi := rf.commitIndex

		logs := make([]EntryData, hi-lo+1)
		copy(logs, rf.log[rf.relIdx(lo):rf.relIdx(hi)+1])
		rf.mu.Unlock()

		for i, entry := range logs {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lo + i,
			}
		}

		rf.mu.Lock()
		if rf.lastApplied < hi {
			rf.lastApplied = hi
		}
	}
}

func (rf *Raft) startElectionState() {
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.dirty = true
	rf.resetElection()
}

func (rf *Raft) attemptElection() {
	rf.mu.Lock()
	if rf.state == LEADER || time.Since(rf.lastHeartBeat) <= rf.electionTimeout {
		rf.mu.Unlock()
		return
	}

	rf.startElectionState()

	term := rf.currentTerm
	lastLogIndex := rf.absIdx(len(rf.log)) - 1
	lastLogTerm := rf.log[len(rf.log)-1].Term
	votedFor := rf.me
	votes := 1

	rf.persistCond.Broadcast()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  votedFor,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			reply := &RequestVoteReply{}

			ok := rf.sendRequestVote(server, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			//Check for stale responses
			if rf.currentTerm != args.Term || rf.state != CANDIDATE {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.stepDownToFollower(reply.Term)
				rf.resetElection()
				rf.persistCond.Broadcast()
				return
			}

			if reply.VoteGranted {
				votes++
			}

			if rf.state == CANDIDATE && votes > len(rf.peers)/2 {
				//Grant vote
				rf.state = LEADER

				//Update next index for peers
				for i := range rf.peers {
					if i == rf.me {
						continue
					}
					rf.nextIndex[i] = rf.absIdx(len(rf.log))
					rf.matchIndex[i] = 0
					rf.lastSent[i] = time.Time{}
					rf.inFlight[i] = false
					rf.replCond[i].Broadcast()
				}
				rf.matchIndex[rf.me] = rf.absIdx(len(rf.log)) - 1
			}
		}(i)
	}

}

func (rf *Raft) batchPersister() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for {
		for !rf.dirty {
			rf.persistCond.Wait()
		}

		persistingLen := len(rf.log)
		stateData := rf.encodeState()
		snapshot := rf.snapshot
		rf.mu.Unlock()

		rf.persister.Save(stateData, snapshot)

		rf.mu.Lock()

		rf.dirty = false

		if persistingLen > rf.persistedLogLen {
			rf.persistedLogLen = persistingLen
		}

		for i := range rf.peers {
			if i != rf.me {
				rf.replCond[i].Broadcast()
			}
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return rf.absIdx(len(rf.log)), rf.currentTerm, rf.state == LEADER
	}

	rf.log = append(rf.log, EntryData{
		Term:    rf.currentTerm,
		Command: command,
	})

	rf.matchIndex[rf.me] = rf.absIdx(len(rf.log)) - 1

	rf.dirty = true
	rf.persistCond.Broadcast()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.replCond[i].Broadcast()
	}

	return rf.absIdx(len(rf.log)) - 1, rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) ticker() {
	for true {
		rf.mu.Lock()
		if rf.state == LEADER {
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				if time.Since(rf.lastSent[i]) >= heartBeatInterval {
					rf.replCond[i].Broadcast()
				}
			}

			rf.mu.Unlock()
		} else {
			timedOut := time.Since(rf.lastHeartBeat) > rf.electionTimeout
			rf.mu.Unlock()
			if timedOut {
				rf.attemptElection()
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.persistCond = sync.NewCond(&rf.mu)
	rf.persistedLogLen = len(rf.log)

	rf.log = make([]EntryData, 1)
	rf.log[0] = EntryData{Term: 0}

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.replCond = make([]*sync.Cond, len(rf.peers))
	rf.lastSent = make([]time.Time, len(rf.peers))
	rf.inFlight = make([]bool, len(rf.peers))
	rf.sendSeq = make([]int, len(rf.peers))

	rf.snapshot = nil
	rf.snapshotIndex = 0
	rf.snapshotTerm = 0

	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.replCond[i] = sync.NewCond(&rf.mu)
	}

	rf.state = FOLLOWER
	rf.resetElection()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = rf.persister.ReadSnapshot()

	if rf.lastApplied < rf.snapshotIndex {
		rf.lastApplied = rf.snapshotIndex
	}

	if rf.commitIndex < rf.snapshotIndex {
		rf.commitIndex = rf.snapshotIndex
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = rf.absIdx(len(rf.log))
		go rf.replicator(i)
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	go rf.batchPersister()
	return rf
}
