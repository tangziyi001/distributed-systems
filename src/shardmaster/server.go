package shardmaster


import "raft"
import "labrpc"
import "sync"
import "encoding/gob"
import "fmt"
import "time"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	// The Key Value store
	SMStore map[string]string
	// The channel for each Client
	SMChan map[int64](chan Op)
	// The lookup map to detect different request for a given index
	SMLookup map[int](Op)
	// Keep track of completed requests
	SMComplete map[int64]int // ClientId, Id
	debug bool
	configs []Config // indexed by config num
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
	ShardArgs interface{}
}

func (sm *ShardMaster) ReplicateEntries(entry Op) bool {
		index, _, isLeader := sm.rf.Start(entry)
		if !isLeader {
			return false
		}
		if sm.debug {
			fmt.Printf("SM Server %d Starts raft with index %d, Op %v, isLeader %v\n", sm.me, index, entry, isLeader)
		}
		if sm.debug {
			fmt.Printf("SM Server %d waits for ID channel for entry %v\n", sm.me, entry)
		}
		// Check if the entry exists
		sm.mu.Lock()
		_, ok := sm.SMChan[entry.ClientId]
		if !ok {
			if sm.debug {
				fmt.Printf("Init Channel for Client ID %d\n", entry.ClientId)
			}
			sm.SMChan[entry.ClientId] = make(chan Op, 1)
		}
		sm.mu.Unlock()
		// Wait for the response
		select {
		case op := <- sm.SMChan[entry.ClientId]:
			if sm.debug {
				fmt.Printf("SM Server %d receive op %v from channl of Client ID %v\n", sm.me, op, entry.ClientId)
			}
			if op.Id == entry.Id {
				break
			} else {
				return false
			}
		case <- time.After(300*time.Millisecond):
			if sm.debug {
				fmt.Printf("SM Server %d channel timeout for op id %v\n", sm.me, entry.ClientId)
			}
			return false
		}

		if sm.debug {
			fmt.Printf("SM Server %d receives apply for Key %v, Command %v\n", sm.me, entry.Key, entry)
		}
		return true
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	entry := Op{Id: args.Id, Action: "Join", ServerId: sm.me, ClientId: args.ClientId, ShardArgs: *args}
	suc := sm.ReplicateEntries(entry)
	if !suc {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}
	sm.mu.Lock()
	if sm.debug {
		fmt.Printf("SM Server %d reply JOIN, op %v\n", sm.me, entry)
	}
	sm.mu.Unlock()

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	entry := Op{Id: args.Id, Action: "Leave", ServerId: sm.me, ClientId: args.ClientId, ShardArgs: *args}
	suc := sm.ReplicateEntries(entry)
	if !suc {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}
	sm.mu.Lock()
	if sm.debug {
		fmt.Printf("SM Server %d reply LEAVE, op %v\n", sm.me, entry)
	}
	sm.mu.Unlock()
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	entry := Op{Id: args.Id, Action: "Move", ServerId: sm.me, ClientId: args.ClientId, ShardArgs: *args}
	suc := sm.ReplicateEntries(entry)
	if !suc {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}
	sm.mu.Lock()
	if sm.debug {
		fmt.Printf("SM Server %d reply MOVE, op %v\n", sm.me, entry)
	}
	sm.mu.Unlock()
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	entry := Op{Id: args.Id, Action: "Query", ServerId: sm.me, ClientId: args.ClientId, ShardArgs: *args}
	suc := sm.ReplicateEntries(entry)
	if !suc {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}
	sm.mu.Lock()
	if args.Num == -1 {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
	if sm.debug {
		fmt.Printf("SM Server %d reply QUERY, config %v\n", sm.me, reply.Config)
	}
	sm.mu.Unlock()
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardsm tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}


func (sm *ShardMaster) rebalance(newConfig Config) Config {
	// Rebalance
	// A map from group to shards (including new groups)
	if sm.debug {
		fmt.Printf("SM Server %d more rebalancing for config %v\n", sm.me, newConfig)
	}
	groupShard := make(map[int][]int)
	shardNum := len(newConfig.Shards)
	groupNum := len(newConfig.Groups)
	for g := range newConfig.Groups {
		groupShard[g] = make([]int, 0)
	}
	for s,g := range newConfig.Shards {
		groupShard[g] = append(groupShard[g], s)
	}
	for {
		maxG := -1
		maxS := -1
		for g,s := range groupShard {
			if len(s) > maxS {
				maxS = len(s)
				maxG = g
			}
		}
		minG := 1000000000
		minS := 1000000000
		for g,s := range groupShard {
			if len(s) < minS {
				minS = len(s)
				minG = g
			}
		}
		// if sm.debug {
		// 	fmt.Printf("SM Server %d rebalancing to %v. maxG %d %d, minG %d %d, s/g %d %d\n", sm.me, groupShard, maxG, maxS, minG, minS, shardNum, groupNum)
		// }
		// Check if another move is unnecessary
		if len(groupShard[minG]) >= (shardNum/groupNum) && len(groupShard[maxG]) <= (shardNum/groupNum) + 1{
			if sm.debug {
				fmt.Printf("SM Server %d rebalancing break\n", sm.me)
			}
			break
		}
		movedShard := groupShard[maxG][0]
		newConfig.Shards[movedShard] = minG
		groupShard[maxG] = groupShard[maxG][1:]
		groupShard[minG] = append(groupShard[minG], movedShard)
	}
	if sm.debug {
		fmt.Printf("SM Server %d finishes rebalancing\n", sm.me)
	}
	return newConfig
}
//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = make(map[int][]string)

	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(JoinReply{})
	gob.Register(LeaveArgs{})
	gob.Register(LeaveReply{})
	gob.Register(MoveArgs{})
	gob.Register(MoveReply{})
	gob.Register(QueryArgs{})
	gob.Register(QueryReply{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.SMStore = make(map[string]string)
	sm.SMChan = make(map[int64](chan Op))
	sm.SMLookup = make(map[int]Op)
	sm.SMComplete = make(map[int64]int)
	sm.debug = false
	// Your code here.
	if sm.debug {
		fmt.Printf("SM Server %d starts\n", sm.me)
	}
	go func(){
		for {
			message := <- sm.applyCh
			if sm.debug {
				fmt.Printf("SM Server %d receives message %v\n", sm.me, message)
			}
			idx := message.Index
			if idx == -1 {
				continue
			}
			op := message.Command.(Op)
			sm.mu.Lock()
			e, ok := sm.SMLookup[idx]
			if ok && (e.Id != op.Id || e.ClientId != op.ClientId) {
				if sm.debug {
					fmt.Printf("SM Server %d detect different request %v against %v for index %d\n", sm.me, e, op, idx)
				}
				sm.mu.Unlock()
				continue
			}
			if !ok {
				sm.SMLookup[idx] = op
			}
			if sm.debug {
				fmt.Printf("SM Server %d SUCCESSFULLY replicated %v\n", sm.me, op)
			}
			val, exist :=  sm.SMComplete[op.ClientId]

			// No dup detected, start new config
			if op.Action != "Query" && (!exist || val < op.Id) {
				// Create new config
				newConfigIdx := len(sm.configs)
				newConfig := Config{Num: newConfigIdx, Shards: sm.configs[newConfigIdx-1].Shards, Groups: make(map[int][]string)}
				// Copy the map
				for k,v := range sm.configs[newConfigIdx-1].Groups {
					newConfig.Groups[k] = v
				}
				newJoined := make([]int, 0)
				if op.Action == "Join" {
					args := op.ShardArgs.(JoinArgs)
					// Add each new group to the config
					for k,v := range args.Servers {
						if _, ok := newConfig.Groups[k]; !ok {
							newConfig.Groups[k] = v
							if sm.debug {
								fmt.Printf("SM JOIN %v Success: Shards %v\n", k, v)
							}
							newJoined = append(newJoined, k)
						} else {
							if sm.debug {
								fmt.Printf("SM JOIN %v Failed: Group Existed\n", k)
							}
						}
					}
					shardNum := len(newConfig.Shards)
					groupNum := len(newConfig.Groups)
					// Join Rebalance
					if newConfig.Shards[0] == 0 {
						if sm.debug {
							fmt.Printf("SM %d init assign %d shards to %d groups\n", sm.me, shardNum, groupNum)
						}
						// Assign all shards to existed groups
						for i := 0; i < shardNum; i++ {
							newConfig.Shards[i] = newJoined[(i % len(newJoined))]
						}
					} else {
						// A map from group to shards (including new groups)
						groupShard := make(map[int][]int)
						for g := range newConfig.Groups {
							groupShard[g] = make([]int, 0)
						}
						for s,g := range newConfig.Shards {
							groupShard[g] = append(groupShard[g], s)
						}

						// At least numMove shards should be transfered
						numMove := (shardNum/groupNum) * len(newJoined)
						for i := 0; i < numMove; i++ {
							maxG := -1
							maxS := -1
							for g,s := range groupShard {
								if len(s) > maxS {
									maxS = len(s)
									maxG = g
								}
							}
							minG := 1000000000
							minS := 1000000000
							for g,s := range groupShard {
								if len(s) < minS {
									minS = len(s)
									minG = g
								}
							}
							movedShard := groupShard[maxG][0]
							newConfig.Shards[movedShard] = minG
							groupShard[maxG] = groupShard[maxG][1:]
							groupShard[minG] = append(groupShard[minG], movedShard)
						}
						newConfig = sm.rebalance(newConfig)
					}
				}
				if op.Action == "Leave" {
					args := op.ShardArgs.(LeaveArgs)
					// Delete each group from config
					for _, k := range args.GIDs {
						if _, ok := newConfig.Groups[k]; ok {
							delete(newConfig.Groups, k)
							if sm.debug {
								fmt.Printf("SM LEAVE %v Success:\n", k)
							}
						} else {
							if sm.debug {
								fmt.Printf("SM LEAVE %v Failed: Group Not Existed\n", k)
							}
						}
					}
					// A map from group to shards (excluding deleted ones)
					groupShard := make(map[int][]int)
					leftShards := make([]int, 0)
					for g := range newConfig.Groups {
						groupShard[g] = make([]int, 0)
					}
					for s,g := range newConfig.Shards {
						if _, ok := groupShard[g]; ok {
							groupShard[g] = append(groupShard[g])
						} else {
							leftShards = append(leftShards, s)
						}
					}
					for _, s := range leftShards {
						minG := 1000000000
						minS := 1000000000
						for g,s := range groupShard {
							if len(s) < minS {
								minS = len(s)
								minG = g
							}
						}
						groupShard[minG] = append(groupShard[minG], s)
						newConfig.Shards[s] = minG
					}
					newConfig = sm.rebalance(newConfig)
				}
				if op.Action == "Move" {
					args := op.ShardArgs.(MoveArgs)
					newConfig.Shards[args.Shard] = args.GID
					if sm.debug {
						fmt.Printf("SM MOVE Shard %d to Group %d Success:\n", args.Shard, args.GID)
					}
				}
				sm.configs = append(sm.configs, newConfig)
				if sm.debug {
					fmt.Printf("***** New Config Created %v\n All configs %v\n", newConfig, sm.configs)
				}
			}
			// Response to the replicate requests
			_, ok = sm.SMChan[op.ClientId]
			if !ok {
				sm.SMChan[op.ClientId] = make(chan Op, 1)
			}
			if sm.debug {
				fmt.Printf("SM Server %d prepares to add command %v to id channel %v\n", sm.me, op, op.Id)
			}
			// Remove an old op
			select {
			case _,ok := <- sm.SMChan[op.ClientId]:
				if ok {
					break
				}
			default:
				if sm.debug {
					fmt.Println("No Op ready")
				}
			}
			sm.SMChan[op.ClientId] <- op
			if sm.debug {
				fmt.Printf("SM Server %d add command %v to id channel %v\n", sm.me, op, op.Id)
			}
			sm.mu.Unlock()
		}
	}()
	return sm
}
