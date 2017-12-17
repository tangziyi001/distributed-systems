package shardkv


import "shardmaster"
import (
	"encoding/gob"
	"bytes"
	"labrpc"
	"raft"
	"sync"
	"fmt"
	"time"
)



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
	GroupId int
	ReconfigArgs interface{}
	PutApplied bool
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	mck					 *shardmaster.Clerk
	config			 shardmaster.Config
	// Your definitions here.
	// Each shard is mapped with a Key Value store
	KVStore [shardmaster.NShards]map[string]string
	// The channel for each Key
	KVChan map[int64](chan Op)
	// The lookup map to detect different request for a given index
	KVLookup map[int](Op)
	// Keep track of completed requests
	KVComplete map[int64]int // ClientId, Id
	debug bool
}

func (kv *ShardKV) ReplicateEntries(entry Op) (bool, Op) {
		index, _, isLeader := kv.rf.Start(entry)
		if !isLeader {
			return false, Op{}
		}
		if kv.debug {
			fmt.Printf("KV Server %d of group %d Starts raft with index %d, Op %v, isLeader %v\n", kv.me, kv.gid, index, entry, isLeader)
		}
		if kv.debug {
			fmt.Printf("KV Server %d of group %d waits for ID channel for entry %v\n", kv.me, kv.gid, entry)
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
		op := Op{}
		// Wait for the response
		select {
		case op = <- kv.KVChan[entry.ClientId]:
			if kv.debug {
				fmt.Printf("KV Server %d of group %d receive op %v from channl of Client ID %v\n", kv.me, kv.gid, op, entry.ClientId)
			}
			if op.Id == entry.Id && op.ClientId == entry.ClientId {
				break
			} else {
				if kv.debug {
					fmt.Printf("KV Server %d of group %d op %v of Client ID %v not match entry %v\n", kv.me, kv.gid, op, entry.ClientId, entry)
				}
				return false, op
			}
		case <- time.After(300*time.Millisecond):
			if kv.debug {
				fmt.Printf("KV Server %d of group %d channel timeout for op id %v\n", kv.me, kv.gid, entry.ClientId)
			}
			return false, op
		}

		if kv.debug {
			fmt.Printf("KV Server %d of group %d receives apply for Key %v, Command %v\n", kv.me, kv.gid, entry.Key, entry)
		}
		return true, op
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{Id: args.Id, Key: args.Key, Value: "", Action: "Get", GroupId: kv.gid, ServerId: kv.me, ClientId: args.ClientId}
	shard := key2shard(args.Key)
	suc, _ := kv.ReplicateEntries(entry)
	if !suc {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}
	kv.mu.Lock()
	var value string
	// Check if the shard belongs to this group
	if kv.config.Shards[shard] == kv.gid {
		value = kv.KVStore[shard][args.Key]
		reply.Value = value
		reply.Err = OK
	} else {
		// This shard doesn't belong to this group
		reply.WrongLeader = false
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.debug {
		fmt.Printf("KV Server %d of group %d reply GET: key %v, value %v\n", kv.me, kv.gid, entry.Key, value)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// Check if the request has already been processed
	kv.mu.Lock()
	val, exist :=  kv.KVComplete[args.ClientId]
	kv.mu.Unlock()
	if exist && val >= args.Id {
		reply.WrongLeader = false
		reply.Err = ErrOutdated
		return
	}
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shard] != kv.gid {
		reply.WrongLeader = false
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	entry := Op{Id: args.Id, Key: args.Key, Value: args.Value, Action: args.Op, GroupId: kv.gid, ServerId: kv.me, ClientId: args.ClientId}
	suc, op := kv.ReplicateEntries(entry)
	kv.mu.Lock()
	if suc {
		reply.WrongLeader = false
		reply.Err = OK
		// Check if Wrong Group
		if kv.gid != kv.config.Shards[shard] && op.PutApplied == false {
			if kv.debug {
				fmt.Printf("KV Server %d of group %d PutAppend WRONG group: key %v, value %v\n", kv.me, kv.gid, entry.Key, entry.Value)
			}
			reply.Err = ErrWrongGroup
		} else {
			if kv.debug {
				fmt.Printf("KV Server %d of group %d reply PutAppend: key %v, value %v\n", kv.me, kv.gid, entry.Key, entry.Value)
			}
		}
	} else {
		reply.WrongLeader = true
	}
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) handleMsg() {
	for {
		message := <- kv.applyCh
		if kv.debug {
			fmt.Printf("KV Server %d of group %d receives msg %v from Raft\n", kv.me, kv.gid, message)
		}

		// Receive a message from Raft
		idx := message.Index
		if idx == -1 && message.UseSnapshot { // Snapshot data is in the command
			if kv.debug {
				fmt.Printf("KV Server %d of group %d applys snapshot\n", kv.me, kv.gid)
			}
			kv.mu.Lock()
			var LastIncludedIndex int
			var LastIncludedTerm int
			r := bytes.NewBuffer(message.Snapshot)
			d := gob.NewDecoder(r)
			d.Decode(&LastIncludedIndex)
			d.Decode(&LastIncludedTerm)
			d.Decode(&kv.KVStore)
			d.Decode(&kv.KVComplete)
			d.Decode(&kv.config)
			// d.Decode(&kv.KVLookup)
			kv.mu.Unlock()
			if kv.debug {
				fmt.Printf("KV Server %d of group %d applys snapshot finished\n", kv.me, kv.gid)
			}
		} else {
			op := message.Command.(Op)
			// Detect lost leadership by checking if index has a different request
			kv.mu.Lock()
			e, ok := kv.KVLookup[idx]
			if ok && (e.Id != op.Id || e.ClientId != op.ClientId) {
				if kv.debug {
					fmt.Printf("KV Server %d of group %d detect different request %v against %v for index %d\n", kv.me, kv.gid, e, op, idx)
				}
				kv.mu.Unlock()
				continue
			}
			if !ok {
				kv.KVLookup[idx] = op
			}
			if kv.debug {
				fmt.Printf("KV Server %d of group %d SUCCESSFULLY replicated %v\n", kv.me, kv.gid, op)
			}

			// Apply Reconfig
			if op.Action == "Reconfig" {
				args := op.ReconfigArgs.(ReconfigArgs)
				if kv.debug {
					fmt.Printf("KV Server %d of group %d original config %d, new config %d\n", kv.me, kv.gid, kv.config.Num, args.Config.Num)
				}
				if args.Config.Num > kv.config.Num {
					for idx, p := range args.Store {
						for k,v := range p {
							kv.KVStore[idx][k] = v
						}
					}
					for clientId := range args.Complete {
						val, exist := kv.KVComplete[clientId]
						if !exist || val < args.Complete[clientId] {
							kv.KVComplete[clientId] = args.Complete[clientId]
						}
					}
					kv.config = args.Config
					if kv.debug {
						fmt.Printf("KV Server %d of group %d Applies Reconfig %v\n", kv.me, kv.gid, kv.config)
					}
				}
			}


			// Apply PutAppend command, check if dup
			val, exist :=  kv.KVComplete[op.ClientId]
			if kv.debug && op.Action == "Append"{
				fmt.Printf("KV Server %d of group %d Append key %v, value %v, config %v, shard %d, target gid %d, KVComplete %v\n", kv.me, kv.gid, op.Key, op.Value, kv.config, key2shard(op.Key), kv.config.Shards[key2shard(op.Key)], kv.KVComplete)
			}
			if (op.Action == "Put" || op.Action == "Append") && (!exist || val < op.Id) && kv.config.Shards[key2shard(op.Key)] == kv.gid {
				shard := key2shard(op.Key)
				kv.KVComplete[op.ClientId] = op.Id
				_, ok := kv.KVStore[shard][op.Key]
				// Check PUT and APPEND
				if !ok {
					kv.KVStore[shard][op.Key] = op.Value
				} else {
					if op.Action == "Put" {
						kv.KVStore[shard][op.Key] = op.Value
					} else {
						kv.KVStore[shard][op.Key] += op.Value
					}
				}
				if kv.debug {
					fmt.Printf("KV Server %d of group %d Applies Client %d's PutAppend key %v, value %v, config %v, shard %d, target gid %d, KVComplete %v\n", kv.me, kv.gid, op.ClientId, op.Key, op.Value, kv.config, key2shard(op.Key), kv.config.Shards[key2shard(op.Key)], kv.KVComplete)
				}
				op.PutApplied = true
			} else if (op.Action == "Put" || op.Action == "Append") && kv.debug {
				fmt.Printf("KV Server %d of group %d unable to put/append from Client %d, op %v. Check shard %v\n", kv.me, kv.gid, op.ClientId, op, kv.config.Shards[key2shard(op.Key)] == kv.gid)
			}

			// Issue snapshot
			if kv.debug {
				fmt.Printf("--- KV Server %d of group %d Check Size: now %d, max %d\n", kv.me, kv.gid, kv.rf.GetRaftStateSize(), kv.maxraftstate)
			}
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				if kv.debug {
					fmt.Printf("--- KV Server %d of group %d issues snapshot ---\n", kv.me, kv.gid)
				}
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(kv.KVStore)
				e.Encode(kv.KVComplete)
				e.Encode(kv.config)
				// e.Encode(kv.KVLookup)
				data := w.Bytes()
				go kv.rf.IssueSnapshot(idx, data)
			}

			// The commit is not requested by this server
			if op.ServerId != kv.me || op.GroupId != kv.gid {
				if kv.debug {
					fmt.Printf("KV Server %d of group %d is not responsible for %v\n", kv.me, kv.gid, op.ServerId)
				}
				kv.mu.Unlock()
				continue
			}

			_, ok = kv.KVChan[op.ClientId]
			if !ok {
				kv.KVChan[op.ClientId] = make(chan Op, 1)
			}
			if kv.debug {
				fmt.Printf("KV Server %d of group %d prepares to add command %v to id channel %v\n", kv.me, kv.gid, op, op.Id)
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
				fmt.Printf("KV Server %d of group %d add command %v to id channel %v\n", kv.me, kv.gid, op, op.Id)
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) sendShardTransfer(gid int, args *TransferArgs, reply *TransferReply) bool {
	// Send to all servers in that group
	for _, server := range kv.config.Groups[gid] {
		if ok := kv.make_end(server).Call("ShardKV.ShardTransfer", args, reply); ok {
			if reply.Err == ErrOutdated {
				// Config is not ready for group gid
				return false
			}
			if reply.Err == OK {
				// Shard Transfer Success with any server in that group.
				return true
			}
		}
	}
	return false
}

func (kv *ShardKV) ShardTransfer(args *TransferArgs, reply *TransferReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Config is outdated
	if kv.config.Num < args.Num {
		reply.Err = ErrOutdated
		return
	}
	reply.Complete = make(map[int64]int)
	for i := 0; i < shardmaster.NShards; i++ {
		reply.Store[i] = make(map[string]string)
	}
	// Transfer Data
	for _, idx := range args.Shards {
		for k,v := range kv.KVStore[idx] {
			// K/V pairs for shard idx
			reply.Store[idx][k] = v
		}
	}
	// Transfer KVComplete
	for clientId := range kv.KVComplete {
		reply.Complete[clientId] = kv.KVComplete[clientId]
	}
	reply.Err = OK
}

func (kv *ShardKV) requestShards(reqShards map[int][]int, args *ReconfigArgs, num int) bool {
	suc := true
	var wg sync.WaitGroup
	var mu sync.Mutex
	for gid, v := range reqShards {
		wg.Add(1)
		// Retrieving shards from each gid
		go func(gid int, v []int) {
			defer wg.Done()
			var reply TransferReply
			ok := kv.sendShardTransfer(gid, &TransferArgs{Num: num, Shards: v}, &reply)
			if ok {
				mu.Lock()
				// Keep track of kv for each shard
				for shardIdx, kv := range reply.Store {
					for k,v := range kv {
						args.Store[shardIdx][k] = v
					}
				}
				// Keep track of KVComplete for each shard
				for clientId := range reply.Complete {
					_, exist := args.Complete[clientId]
					if !exist || args.Complete[clientId] < reply.Complete[clientId] {
						args.Complete[clientId] = reply.Complete[clientId]
					}
				}
				mu.Unlock()
			} else {
				suc = false
			}
		}(gid, v)
	}
	wg.Wait()
	return suc
}

func (kv *ShardKV) handleConfig() {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			config := kv.mck.Query(-1)
			// Process all new configs
			start := kv.config.Num + 1
			end := config.Num + 1
			for k := start; k < end; k++ {
				if kv.debug {
					fmt.Printf("KV Server %d of group %d starts config change %d\n", kv.me, kv.gid, k)
				}
				nowConfig := kv.mck.Query(k)
				args := ReconfigArgs{Config: nowConfig, Complete: make(map[int64]int)}
				for i := 0; i < shardmaster.NShards; i++ {
					// Keep track of KV for each shard
					args.Store[i] = make(map[string]string)
				}
				// Keep track of the requested shards for each group
				kv.mu.Lock()
				reqShards := make(map[int][]int)
				for i := 0; i < shardmaster.NShards; i++ {
					// Shard that's needed to retrieve is detected
					if kv.config.Shards[i] != kv.gid && nowConfig.Shards[i] == kv.gid {
						if kv.debug {
							fmt.Printf("KV Server %d of group %d detect pending retrieving shard %d during config change to %d\n", kv.me, kv.gid, i, k)
						}
						old_gid := kv.config.Shards[i]
						if old_gid != 0 {
							if _, ok := reqShards[old_gid]; !ok {
								reqShards[old_gid] = []int{}
							}
							reqShards[old_gid] = append(reqShards[old_gid], i)
						}
					}
				}
				kv.mu.Unlock()
				// Check if shards data is successfully retrieved
				// Args has been updated
				suc := kv.requestShards(reqShards, &args, nowConfig.Num)
				if !suc {
					break
				}
				// Replicate to Raft
				entry := Op{ClientId: 0, Id: nowConfig.Num, GroupId: kv.gid, ServerId: kv.me, Action: "Reconfig", ReconfigArgs: args}
				if kv.debug {
					fmt.Printf("KV Server %d of group %d Reconfig Preparation Completed\n", kv.me, kv.gid)
				}
				suc, _ = kv.ReplicateEntries(entry)
				// New config has successfully replicated
				if !suc {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(ReconfigArgs{})
	gob.Register(ReconfigReply{})
	gob.Register(TransferArgs{})
	gob.Register(TransferReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.debug = false
	for i := 0; i < shardmaster.NShards; i++ {
		kv.KVStore[i] = make(map[string]string)
	}
	kv.KVChan = make(map[int64](chan Op))
	kv.KVLookup = make(map[int]Op)
	kv.KVComplete = make(map[int64]int)
	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	if kv.debug {
		fmt.Printf("KV Server %d of group %d starts\n", kv.me, gid)
	}

	go kv.handleMsg()
	go kv.handleConfig()
	return kv
}
