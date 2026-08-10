package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

const (
	UNLOCKED = 0
	LOCKED   = 1
)

type DistributedLock struct {
	workerId  int
	lockState int
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVData struct {
	val     string
	version rpc.Tversion
}

type KVServer struct {
	mu      sync.Mutex
	version rpc.Tversion
	data    map[string]KVData
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		data: make(map[string]KVData),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	data, ok := kv.data[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = data.val
	reply.Version = data.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	data, ok := kv.data[args.Key]

	if !ok {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}

		kv.data[args.Key] = KVData{
			val:     args.Value,
			version: 1,
		}
		reply.Err = rpc.OK
		return
	}

	if data.version != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	kv.data[args.Key] = KVData{
		val:     args.Value,
		version: data.version + 1,
	}

	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
