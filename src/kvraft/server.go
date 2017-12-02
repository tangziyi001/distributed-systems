package raftkv

import (
	"encoding/gob"
	"bytes"
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
	Id int
	Key	string
	Value string
	Action string
	ServerId int
	ClientId int64
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
	// The lookup map to detect different request for a given index
	KVLookup map[int](Op)
	// Keep track of completed requests
	KVComplete map[int64]int // ClientId, Id
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
		_, ok := kv.KVChan[entry.ClientId]
		if !ok {
			if kv.debug {
				fmt.Printf("Init Channel for Client ID %d\n", entry.ClientId)
			}
			kv.KVChan[entry.ClientId] = make(chan Op, 1)
		}
		kv.mu.Unlock()
		// Wait for the response
		select {
		case op := <- kv.KVChan[entry.ClientId]:
			if kv.debug {
				fmt.Printf("KV Server %d receive op %v from channl of Client ID %v\n", kv.me, op, entry.ClientId)
			}
			if op == entry {
				break
			} else {
				return false
			}
		case <- time.After(300*time.Millisecond):
			if kv.debug {
				fmt.Printf("KV Server %d channel timeout for op id %v\n", kv.me, entry.ClientId)
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
	entry := Op{Id: args.Id, Key: args.Key, Value: "", Action: "Get", ServerId: kv.me, ClientId: args.ClientId}
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
	val, exist :=  kv.KVComplete[args.ClientId]
	kv.mu.Unlock()
	if exist && val >= args.Id {
		reply.WrongLeader = false
		return
	}
	entry := Op{Id: args.Id, Key: args.Key, Value: args.Value, Action: args.Op, ServerId: kv.me, ClientId: args.ClientId}
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
	// Your code here, if desired
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
				fmt.Printf("KV Server %d receives msg from Raft\n", kv.me)
			}

			// Receive a message from Raft
			idx := message.Index
			if idx == -1 && message.UseSnapshot { // Snapshot data is in the command
				if kv.debug {
					fmt.Printf("Server %d applys snapshot\n", kv.me)
				}
				var LastIncludedIndex int
				var LastIncludedTerm int
				r := bytes.NewBuffer(message.Snapshot)
				d := gob.NewDecoder(r)
				d.Decode(&LastIncludedIndex)
				d.Decode(&LastIncludedTerm)
				kv.mu.Lock()
				d.Decode(&kv.KVStore)
				d.Decode(&kv.KVComplete)
				// d.Decode(&kv.KVLookup)
				kv.mu.Unlock()
			} else {
				op := message.Command.(Op)
				// Detect lost leadership by checking if index has a different request
				kv.mu.Lock()
				e, ok := kv.KVLookup[idx]
				if ok && e != op {
					if kv.debug {
						fmt.Printf("KV Server %d detect different request %v against %v for index %d\n", kv.me, e, op, idx)
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

				// Apply PutAppend command, check if dup
				val, exist :=  kv.KVComplete[op.ClientId]
				if op.Action != "Get" && (!exist || val < op.Id) {
					kv.KVComplete[op.ClientId] = op.Id
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
					if kv.debug {
						fmt.Printf("KV Server %d Applies Client %d's PutAppend op %v\n", kv.me, op.ClientId, op)
					}
				} else if op.Action != "Get" && kv.debug {
					fmt.Printf("KV Server %d detects dup for Client %d, op %v\n", kv.me, op.ClientId, op)
				}

				// Issue snapshot
				if kv.debug {
					fmt.Printf("--- KV Server %d Check Size: now %d, max %d\n", kv.me, kv.rf.GetRaftStateSize(), maxraftstate)
				}
				if maxraftstate != -1 && kv.rf.GetRaftStateSize() > maxraftstate {
					if kv.debug {
						fmt.Printf("--- KV Server %d issues snapshot ---\n", kv.me)
					}
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.KVStore)
					e.Encode(kv.KVComplete)
					// e.Encode(kv.KVLookup)
					data := w.Bytes()
					go kv.rf.IssueSnapshot(idx, data)
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
				_, ok = kv.KVChan[op.ClientId]
				if !ok {
					kv.KVChan[op.ClientId] = make(chan Op, 1)
				}
				if kv.debug {
					fmt.Printf("KV Server %d prepares to add command %v to id channel %v\n", kv.me, op, op.Id)
				}
				// Remove an old op
				select {
				case _,ok := <- kv.KVChan[op.ClientId]:
					if ok {
						break
					}
				default:
					if kv.debug {
						fmt.Println("No Op ready")
					}
				}
				kv.KVChan[op.ClientId] <- op
				if kv.debug {
					fmt.Printf("KV Server %d add command %v to id channel %v\n", kv.me, op, op.Id)
				}
				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
