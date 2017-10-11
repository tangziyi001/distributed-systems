package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "fmt"
// import "bytes"
// import "encoding/gob"
import "math/rand"
import "time"

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Entry of log
type Entry struct {
	Command		interface{}
	Term		int
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Server State
	state int
	timeout int

	// Persistent States
	currentTerm	int
	votedFor	int
	log		[]Entry

	// Volatile States
	commitIndex	int
	lastApplied	int

	// Leaders States
	nextIndex	[]int
	matchIndex	[]int
}

// Get min from two integers
func min(x,y int) int {
	if x < y {
		return x
	}
	return y
}

// Generate a random timeout in milliseconds
func randTimeOut(base int) time.Duration {
	return time.Duration(rand.Intn(300)+base)*time.Millisecond
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}



//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term		int
	CandidateId	int
	LastLogIndex	int
	LastLogTerm	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term		int
	VoteGranted	bool
}

type AppendEntriesArgs struct {
	Term		int
	LeaderId	int
	PrevLogIndex	int
	PrevLogTerm	int
	Entries		[]Entry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term		int
	Success		bool
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	fmt.Printf("RequestVote Sent from %d (Term %d) to %d (currentTerm %d)\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}
	if args.Term < rf.currentTerm {
		fmt.Printf("RequestVote Failed: Term Outdated; currentTerm %d, args Term %d\n", rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	var updated bool
	var lastLogTerm = rf.log[len(rf.log)-1].Term
	var lastLogIndex = len(rf.log)-1
	updated = args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && updated {
		fmt.Printf("RequestVote Success from %d to %d\n", args.CandidateId, rf.me)
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			fmt.Printf("RequestVote %d-%d Failed: vote used; votedFor %d\n", args.CandidateId, rf.me, rf.votedFor)
		} else {
			fmt.Printf("RequestVote %d-%d Failed: Log outdated: (arg term: %d arg index: %d) (cur term: %d cur index: %d)\n", args.CandidateId, rf.me, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
		}
	}
	rf.RequestVoteLog(args, reply)
}

func (rf *Raft) RequestVoteLog(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Printf(
		`
		--- RequestVote Handler for Server %d ---
		RequestVote Send from %d to %d
		Server current states:
			currentTerm: %d
			votedFor: %d
			commitIndex: %d
			lastApplied: %d
			log: %v
		Args:
			Term: %d
			CandidateId: %d
			LastLogTerm: %d
			LastLogIndex: %d
		Reply:
			Term: %d
			VoteGranted: %d

		`,
		rf.me,
		args.CandidateId,
		rf.me,
		rf.currentTerm,
		rf.votedFor,
		rf.commitIndex,
		rf.lastApplied,
		rf.log,
		args.Term,
		args.CandidateId,
		args.LastLogTerm,
		args.LastLogIndex,
		reply.Term,
		reply.VoteGranted)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) issueRequestVote() {
	fmt.Printf("Server %d issues RequestVote in term %d\n", rf.me, rf.currentTerm)
	workChan := make(chan *RequestVoteReply, len(rf.peers))
	for n := 0; n < len(rf.peers); n++ {
		if n == rf.me {
			continue
		}
		go func(i int) {
			args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: len(rf.log)-1, LastLogTerm: rf.log[len(rf.log)-1].Term}
			var reply RequestVoteReply
			res := rf.sendRequestVote(i, &args, &reply)
			if res == true {
				workChan <- &reply
			} else {
				workChan <- nil
			}
		}(n)
	}

	go func(){
		var successReplies []*RequestVoteReply
		var nReplies int
		majority := len(rf.peers)/2
		for r := range workChan {
			nReplies++
			if r != nil && r.VoteGranted {
				successReplies = append(successReplies, r)
			}
			if nReplies == len(rf.peers)-1 || len(successReplies) == majority {
				break
			}
		}
		if len(successReplies) >= majority {
			rf.mu.Lock()
			fmt.Printf("CANDIDATE SUCCESS %d: get votes %d\n", rf.me, len(successReplies)+1)
			// Become Leader
			rf.state = LEADER
			go rf.issueAppendEntries()
			rf.mu.Unlock()
		} else {
			fmt.Printf("CANDIDATE FAILED %d: get votes %d\n", rf.me, len(successReplies)+1)
		}
	}()
}

// Handler for AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("AppendEntries Sent from %d (Term %d) to %d (currentTerm %d)\n", args.LeaderId, args.Term, rf.me, rf.currentTerm)
	rf.timeout = 0
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	sz := min(len(rf.log), len(args.Entries))
	idx := sz
	// Find conflict entry
	for i := 0; i < sz; i++ {
		if rf.log[i] != args.Entries[i] {
			idx = i;
			break
		}
	}
	// Delete this entry and all that follow it
	rf.log = rf.log[:idx]
	// Append logs
	for i := idx; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.commitIndex)
	}

	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied = rf.commitIndex
		// Apply committed commands to the state machine
	}
	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntriesLog(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Printf(
		`
		--- AppendEntries Handler for Server %d ---
		RequestVote Send from %d to %d
		Server current states:
			currentTerm: %d
			votedFor: %d
			commitIndex: %d
			lastApplied: %d
			log: %v
		Args:
			Term: %d
			LeaderId: %d
			PrevLogIndex: %d
			PrevLogTerm: %d
			Entries: %v
			LeaderCommit: %d
		Reply:
			Term: %d
			Success: %d

		`,
		rf.me,
		args.LeaderId,
		rf.me,
		rf.currentTerm,
		rf.votedFor,
		rf.commitIndex,
		rf.lastApplied,
		rf.log,
		args.Term,
		args.LeaderId,
		args.PrevLogIndex,
		args.PrevLogTerm,
		args.Entries,
		args.LeaderCommit,
		reply.Term,
		reply.Success)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) issueAppendEntries() {
	workChan := make(chan *AppendEntriesReply, len(rf.peers))
	for n := 0; n < len(rf.peers); n++ {
		if n == rf.me {
			continue
		}
		go func(i int) {
			args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: len(rf.log)-1, PrevLogTerm: rf.log[len(rf.log)-1].Term, Entries: rf.log, LeaderCommit: rf.commitIndex}
			var reply AppendEntriesReply
			res := rf.sendAppendEntries(i, &args, &reply)
			if res == true {
				workChan <- &reply
			} else {
				workChan <- nil
			}
		}(n)
	}

	go func(){
		var successReplies []*AppendEntriesReply
		var nReplies int
		majority := len(rf.peers)/2
		for r := range workChan {
			nReplies++
			if r != nil {
				successReplies = append(successReplies, r)
			}
			if nReplies == len(rf.peers)-1 || len(successReplies) == majority {
				break
			}
		}
		if len(successReplies) >= majority {
			// TODO
		}
	}()
}
//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	isLeader = (rf.state == LEADER)

	if isLeader == true {
		index = len(rf.log)+1
		term = rf.currentTerm
		rf.log = append(rf.log, Entry{Command: command, Term: rf.currentTerm})
	}
	fmt.Printf("Start command %v on server %d (%v), index %d, term %d, rf_log %v\n", command, rf.me, rf.state, index, term, rf.log)
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = FOLLOWER
	// Placeholder Entry at index 0
	rf.log = append(rf.log, Entry{Term: 0})
	rf.votedFor = -1
	rf.timeout = 1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	fmt.Printf(
		`
		Make rf %d
			currentTerm %d
			state %d
			votedFor %d
			log %v
		`,
		rf.me,
		rf.currentTerm,
		rf.state,
		rf.votedFor,
		rf.log)

	go func(){
		for {
			rf.mu.Lock()
			if rf.state == FOLLOWER {
				if rf.timeout == 1 && rf.votedFor == -1{
					fmt.Printf("FOLLOWER %d Timeout: Become Candidate\n", rf.me)
					rf.timeout = 0
					rf.state = CANDIDATE
					// Start Vote
					rf.currentTerm += 1
					rf.votedFor = rf.me
					rf.mu.Unlock()
					go rf.issueRequestVote()
				} else {
					rf.timeout = 1
					rf.mu.Unlock()
					time.Sleep(randTimeOut(1000))
				}
			}
			if rf.state == CANDIDATE {
				if rf.timeout == 1 && rf.votedFor == -1{
					fmt.Printf("CANDIDATE %d Timeout: Become Candidate\n", rf.me)
					rf.currentTerm += 1
					rf.timeout = 0
					rf.votedFor = rf.me
					rf.mu.Unlock()
					go rf.issueRequestVote()
				} else {
					rf.timeout = 1
					rf.votedFor = -1
					rf.mu.Unlock()
					time.Sleep(randTimeOut(1000))
				}
			}
			if rf.state == LEADER {
				rf.mu.Unlock()
				go rf.issueAppendEntries()
				time.Sleep(randTimeOut(200))
			}
		}
	}()
	return rf
}
