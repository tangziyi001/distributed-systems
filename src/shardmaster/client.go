package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"
import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck := new(Clerk)
	ck.servers = servers
	ck.debug = false
	ck.count = 0
	ck.lastLeader = -1
	ck.id = nrand()
	ck.count = 0
	return ck
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{}
	args.Num = num
	// Your code here.
	ck.mu.Lock()
	id := ck.count
	ck.count += 1
	args.Id = id
	args.ClientId = ck.id
	ck.mu.Unlock()
	if ck.debug {
		fmt.Printf("\nClient Query, args %v, id %v\n", args, id)
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", &args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{}
	// Your code here.
	args.Servers = servers
	ck.mu.Lock()
	id := ck.count
	ck.count += 1
	args.Id = id
	args.ClientId = ck.id
	ck.mu.Unlock()
	if ck.debug {
		fmt.Printf("\nClient Join, args %v, id %v\n", args, id)
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", &args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	ck.mu.Lock()
	id := ck.count
	ck.count += 1
	args.Id = id
	args.ClientId = ck.id
	ck.mu.Unlock()
	if ck.debug {
		fmt.Printf("\nClient Leave, gids %v, id %v\n", gids, id)
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", &args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	ck.mu.Lock()
	id := ck.count
	ck.count += 1
	args.Id = id
	args.ClientId = ck.id
	ck.mu.Unlock()
	if ck.debug {
		fmt.Printf("\nClient Move, shard %d, gid %d, id %v\n", shard, gid, id)
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", &args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
