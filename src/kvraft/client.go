package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "fmt"
// import "time"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	debug bool
	lastLeader int // Keep track of the leader of the last RPC
	id int64
	count int
	mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	// You'll have to add code here.
	ck := new(Clerk)
	ck.servers = servers
	ck.debug = false
	ck.count = 0
	ck.lastLeader = -1
	ck.id = nrand()
	ck.count = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	id := ck.count
	ck.count += 1
	args := GetArgs{Key:key, Id:id, ClientId: ck.id}
	ck.mu.Unlock()
	retry := 0
	leader := -1
	if ck.debug {
		fmt.Printf("\nClient Get, key %v, id %v\n", key, id)
	}
	for {
		var reply GetReply
		ck.mu.Lock()
		if ck.lastLeader != -1 && leader == -1 {
			leader = ck.lastLeader
		} else if leader == -1 {
			leader = 0
		}
		ck.mu.Unlock()
		ok := ck.servers[leader].Call("RaftKV.Get", &args, &reply)
		if !ok  || reply.WrongLeader {
			retry += 1
			leader += 1
			leader %= len(ck.servers)
			continue
		} else {
			if ck.debug {
				fmt.Printf("Client Get Success, args %v, reply %v\n\n", args, reply)
			}
			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	id := ck.count
	ck.count += 1
	args := PutAppendArgs{Key:key, Value:value, Op:op, Id: id, ClientId: ck.id}
	ck.mu.Unlock()
	retry := 0
	leader := -1
	if ck.debug {
		fmt.Printf("\nClient PutAppends, key %v, value %v, id %v\n", key, value, id)
	}
	for {
		var reply PutAppendReply
		ck.mu.Lock()
		if ck.lastLeader != -1 && leader == -1 {
			leader = ck.lastLeader
		} else if leader == -1 {
			leader = 0
		}
		ck.mu.Unlock()
		ok := ck.servers[leader].Call("RaftKV.PutAppend", &args, &reply)
		// // Handle Request Timeout
		// req := make(chan bool, 1)
		// go func () {
		// 	req <- ck.servers[leader].Call("RaftKV.PutAppend", &args, &reply)
		// }()
		// var ok bool
		// select {
		// case ok = <- req:
		// 	// if ck.debug {
		// 	// 	fmt.Printf("Client PutAppend OK, args %v, reply %v\n", args, reply)
		// 	// }
		// case <- time.After(500*time.Millisecond):
		// 	if ck.debug {
		// 		fmt.Printf("Client PutAppend Timeout, args %v, reply %v, leader %d\n", args, reply, leader)
		// 	}
		// 	ok = false
		// }
		if !ok  || reply.WrongLeader == true {
			retry += 1
			leader += 1
			leader %= len(ck.servers)
			// if ck.debug {
			// 	fmt.Printf("Client PutAppend Retry Wrong Leader, args %v, reply %v\n", args, reply)
			// }
			continue
		} else {
			ck.mu.Lock()
			ck.lastLeader = leader
			if ck.debug {
				fmt.Printf("Client PutAppend Success, args %v, reply %v\n\n", args, reply)
			}
			ck.mu.Unlock()
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
