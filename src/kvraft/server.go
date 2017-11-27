package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"fmt"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64
	Key	string
	Value string
	Action string
	ServerId int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// The Key Value store
	KVStore map[string]string
	// The channel for each Key
	KVChan map[int64](chan Op)
	// The lookup map to detect different request
	KVLookup map[int](Op)
	// Keep track of completed requests
	KVComplete map[int64]int
	debug bool
}

func (kv *RaftKV) ReplicateEntries(entry Op) bool {
		index, _, isLeader := kv.rf.Start(entry)
		if !isLeader {
			return false
		}
		if kv.debug {
			fmt.Printf("KV Server %d Starts raft with index %d, Op %v, isLeader %v\n", kv.me, index, entry, isLeader)
		}
		if kv.debug {
			fmt.Printf("KV Server %d waits for ID channel for entry %v\n", kv.me, entry)
		}
		// Check if the entry exists
		kv.mu.Lock()
		_, ok := kv.KVChan[entry.Id]
		if !ok {
			if kv.debug {
				fmt.Printf("Init Channel for ID %d\n", entry.Id)
			}
			kv.KVChan[entry.Id] = make(chan Op, 1)
		}
		kv.mu.Unlock()
		// Wait for the response
		select {
		case op := <- kv.KVChan[entry.Id]:
			if kv.debug {
				fmt.Printf("KV Server %d receive op %v from channl of ID %v\n", kv.me, op, entry.Id)
			}
			if op == entry {
				break
			} else {
				return false
			}
		case <- time.After(300*time.Millisecond):
			if kv.debug {
				fmt.Printf("KV Server %d channel timeout for op id %v\n", kv.me, entry.Id)
			}
			return false
		}

		if kv.debug {
			fmt.Printf("KV Server %d receives apply for Key %v, Command %v\n", kv.me, entry.Key, entry)
		}
		return true
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{Id: args.Id, Key: args.Key, Value: "", Action: "Get", ServerId: kv.me}
	suc := kv.ReplicateEntries(entry)
	if !suc {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}
	kv.mu.Lock()
	value := kv.KVStore[args.Key]
	reply.Value = value
	if kv.debug {
		fmt.Printf("KV Server %d reply GET: key %v, value %v\n", kv.me, entry.Key, value)
	}
	kv.mu.Unlock()
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// Check if the request has already been processed
	kv.mu.Lock()
	_, exist :=  kv.KVComplete[args.Id]
	kv.mu.Unlock()
	if exist {
		reply.WrongLeader = false
		return
	}
	entry := Op{Id: args.Id, Key: args.Key, Value: args.Value, Action: args.Op, ServerId: kv.me}
	suc := kv.ReplicateEntries(entry)
	if suc {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.debug = false
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.KVStore = make(map[string]string)
	kv.KVChan = make(map[int64](chan Op))
	kv.KVLookup = make(map[int]Op)
	kv.KVComplete = make(map[int64]int)
	// You may need initialization code here.
	// Read applyCh
	if kv.debug {
		fmt.Printf("KV Server %d starts\n", kv.me)
	}
	go func(){
		for {
			message := <- kv.applyCh
			if kv.debug {
				fmt.Printf("KV Server %d receives msg from Raft %v\n", kv.me, message)
			}

			// Receive a message from Raft
			op := message.Command.(Op)
			idx := message.Index

			// Detect lost leadership by checking if index has a different request
			kv.mu.Lock()
			val, ok := kv.KVLookup[idx]
			if ok && val != op {
				if kv.debug {
					fmt.Printf("KV Server %d detect different request %v against %v for index %d\n", kv.me, val, op, idx)
				}
				kv.mu.Unlock()
				continue
			}
			if !ok {
				kv.KVLookup[idx] = op
			}
			if kv.debug {
				fmt.Printf("KV Server %d SUCCESSFULLY replicated %v\n", kv.me, op)
			}

			// Apply PutAppend command
			_, exist :=  kv.KVComplete[op.Id]
			if op.Action != "Get" && !exist {
				kv.KVComplete[op.Id] = 1
				_, ok := kv.KVStore[op.Key]
				// Check PUT and APPEND
				if !ok {
					kv.KVStore[op.Key] = op.Value
				} else {
					if op.Action == "Put" {
						kv.KVStore[op.Key] = op.Value
					} else {
						kv.KVStore[op.Key] += op.Value
					}
				}
			}

			// The commit is not requested by this server
			if op.ServerId != kv.me {
				if kv.debug {
					fmt.Printf("KV Server %d is not responsible for %v\n", kv.me, op.ServerId)
				}
				kv.mu.Unlock()
				continue
			}
			kv.mu.Unlock()

			// Response to the replicate requests
			kv.mu.Lock()
			_, ok = kv.KVChan[op.Id]
			if !ok {
				kv.KVChan[op.Id] = make(chan Op, 1)
			}
			kv.mu.Unlock()
			if kv.debug {
				fmt.Printf("KV Server %d prepares to add command %v to id channel %v\n", kv.me, op, op.Id)
			}
			// Remove an old op
			select {
			case _,ok := <- kv.KVChan[op.Id]:
				if ok {
					break
				}
			default:
				if kv.debug {
					fmt.Println("No Op ready")
				}
			}
			kv.KVChan[op.Id] <- op
			if kv.debug {
				fmt.Printf("KV Server %d add command %v to id channel %v\n", kv.me, op, op.Id)
			}
		}
	}()

	return kv
}
