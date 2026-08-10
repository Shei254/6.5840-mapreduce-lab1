package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here

	lockName string
	clientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here

	lk.lockName = lockname
	lk.clientID = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.lockName)

		//doent t
		if err == rpc.ErrNoKey || val == "" {
			lk.ck.Put(lk.lockName, lk.clientID, ver)
		}

		val2, _, _ := lk.ck.Get(lk.lockName)
		if val2 == lk.clientID {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, ver, _ := lk.ck.Get(lk.lockName)
		if val == lk.clientID {
			lk.ck.Put(lk.lockName, "", ver)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

}
