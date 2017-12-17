package shardkv

import "shardmaster"
//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrOutdated 	= "ErrOutdated"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Id int
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ClientId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Id int
	Key string
	ClientId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type ReconfigArgs struct {
	Config			shardmaster.Config
	Store				[shardmaster.NShards]map[string]string
	Complete		map[int64]int
}

type ReconfigReply struct {
	WrongLeader	bool
}

type TransferArgs struct {
	Num					int
	Shards			[]int
}

type TransferReply struct {
	Err					Err
	Store [shardmaster.NShards]map[string]string
	Complete map[int64]int
}
