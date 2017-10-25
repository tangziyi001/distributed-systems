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
	debug bool
	commitChan chan ApplyMsg

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
	if rf.debug {
		fmt.Printf("RequestVote Sent from %d (Term %d) to %d (currentTerm %d)\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	}
	// Step Down
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}
	if args.Term < rf.currentTerm {
		if rf.debug {
			fmt.Printf("RequestVote Failed: Term Outdated; currentTerm %d, args Term %d\n", rf.currentTerm, args.Term)
		}
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
		if rf.debug {
			fmt.Printf("RequestVote Success from %d to %d\n", args.CandidateId, rf.me)
		}
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			if rf.debug {
				fmt.Printf("RequestVote %d-%d Failed: vote used; votedFor %d\n", args.CandidateId, rf.me, rf.votedFor)
			}
		} else {
			if rf.debug {
				fmt.Printf("RequestVote %d-%d Failed: Log outdated: (arg term: %d arg index: %d) (cur term: %d cur index: %d)\n", args.CandidateId, rf.me, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
			}
		}
	}
	if rf.debug {
		rf.RequestVoteLog(args, reply)
	}
}

func (rf *Raft) RequestVoteLog(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.debug {
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
	if rf.debug {
		fmt.Printf("Server %d issues RequestVote in term %d\n", rf.me, rf.currentTerm)
	}
	workChan := make(chan *RequestVoteReply, len(rf.peers))
	rf.mu.Lock()
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: len(rf.log)-1, LastLogTerm: rf.log[len(rf.log)-1].Term}
	rf.mu.Unlock()
	for n := 0; n < len(rf.peers); n++ {
		if n == rf.me {
			continue
		}
		go func(i int) {
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
		maxTerm := rf.currentTerm
		for r := range workChan {
			nReplies++
			if r != nil && r.VoteGranted {
				successReplies = append(successReplies, r)
				if r.Term > maxTerm {
					maxTerm = r.Term
				}
			}
			if nReplies == len(rf.peers)-1 || len(successReplies) >= majority {
				break
			}
		}
		rf.mu.Lock()
		if maxTerm > rf.currentTerm {
			rf.currentTerm = maxTerm
			rf.state = FOLLOWER
			rf.timeout = 0
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		if len(successReplies) >= majority {
			rf.mu.Lock()
			if rf.debug {
				fmt.Printf("CANDIDATE SUCCESS %d: get votes %d\n", rf.me, len(successReplies)+1)
			}
			// Become Leader
			rf.state = LEADER
			// Reinitialize leader states
			rf.leaderInit()
			go rf.issueAppendEntries(false)
			rf.mu.Unlock()
		} else {
			if rf.debug {
				fmt.Printf("CANDIDATE FAILED %d: get votes %d\n", rf.me, len(successReplies)+1)
			}
		}
	}()
}

// Handler for AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.debug {
		fmt.Printf("AppendEntries Sent from %d (Term %d) to %d (currentTerm %d)\n", args.LeaderId, args.Term, rf.me, rf.currentTerm)
	}
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.timeout = 0
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}


	// Log Inconsistent
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Truncate the log
		if args.PrevLogIndex < len(rf.log) {
			rf.log = rf.log[:args.PrevLogIndex]
		}
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// For non-HeartBeat only
	if args.Entries != nil {
		// Overwrite and append
		for i := args.PrevLogIndex+1; i < len(args.Entries); i++ {
			if i >= len(rf.log) {
				rf.log = append(rf.log, args.Entries[i])
			} else {
				rf.log[i] = args.Entries[i]
			}
		}
	}

	// Update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	if rf.debug {
		fmt.Printf("Updated log for server %d: \n %v\n", rf.me, rf.log)
		fmt.Printf("Commit Index %d; Last Appled %d, LeaderCommit %d\n", rf.commitIndex, rf.lastApplied, args.LeaderCommit)
	}

	// Apply commands
	if rf.commitIndex > rf.lastApplied {
		// Apply lastApplied to the state machine
		for i := rf.lastApplied+1; i <= rf.commitIndex; i++ {
			if rf.debug {
				fmt.Printf("Updated lastApplied for server %d: \n %v\n", rf.me, i)
			}
			rf.mu.Unlock()
			rf.commitChan <- ApplyMsg{Index: i, Command: rf.log[i].Command}
			rf.mu.Lock()
		}
		rf.lastApplied = rf.commitIndex
		if rf.debug {
			fmt.Printf("-----Applied Server %d commands to %d-----\n", rf.me, rf.lastApplied)
		}
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	// if rf.debug {
	// 	rf.AppendEntriesLog(args, reply)
	// }
}

func (rf *Raft) AppendEntriesLog(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.debug {
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
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Issue AppendEntries to one server
func (rf *Raft) issueSingleAppendEntries(i int, curTerm int, hb bool) {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	nextIdx := rf.nextIndex[i]
	args := AppendEntriesArgs{Term: curTerm, LeaderId: rf.me, PrevLogIndex: nextIdx-1, PrevLogTerm: rf.log[nextIdx-1].Term, Entries: nil, LeaderCommit: rf.commitIndex}
	if !hb || len(rf.log)-1 >= nextIdx {
		args.Entries = rf.log
	}
	rf.mu.Unlock()
	var reply AppendEntriesReply
	// Retry number for debug
	retry := 0
	// Repeatly send requests
	for {
		rf.mu.Lock()
		// Other reply might reset the state
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		if rf.debug {
			fmt.Printf("Issue AppendEntries from %d (Term %d) to %d, Retry %d, HB %v\n log %v\n", rf.me, rf.currentTerm, i, retry, hb, args.Entries)
			fmt.Printf("nextIndex: %v\nmatchIndex: %v\n", rf.nextIndex, rf.matchIndex)
			retry = retry + 1
		}
		rf.mu.Unlock()
		res := rf.sendAppendEntries(i, &args, &reply)
		if res == true {
			rf.mu.Lock()
			// Term outdated. Become FOLLOWER.
			if reply.Term > rf.currentTerm {
				if rf.debug {
					fmt.Printf("Term Outdated for leader after issueAppendEntries for server %d in term %d\n", rf.me, rf.currentTerm)
				}
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				rf.timeout = 0
				rf.votedFor = -1
				rf.mu.Unlock()
				return
			}
			// The request is outdated
			if reply.Term > args.Term {
				rf.mu.Unlock()
				return
			}
			// Discover Log inconsistent
			if reply.Success == false {
				if rf.debug {
					fmt.Printf("Decrement NextIndex for server %d from %d\n Leader Log %v\n", i, rf.nextIndex[i], args.Entries)
				}
				// Decrement nextIndex and Retry
				rf.nextIndex[i] = rf.nextIndex[i]-1
				args.PrevLogIndex = rf.nextIndex[i]-1
				if args.PrevLogIndex < 0 {
					rf.mu.Unlock()
					return
				}
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				rf.mu.Unlock()
				continue
			} else if reply.Success {
				// Heartbeat. Don't update anything
				if args.Entries == nil {
					rf.mu.Unlock()
					return
				}
				if rf.debug {
					fmt.Printf("Successfully Replicated from server %d to %d, lastIndex %d\n", rf.me, i, len(rf.log)-1)
				}
				// Update matchIndex and nextIndex
				rf.matchIndex[i] = len(rf.log)-1
				rf.nextIndex[i] = len(rf.log)
				rf.mu.Unlock()
				return
			} else {
				// Unlock and send again
				rf.mu.Unlock()
			}
		}
	}
}

// Issue AppendEntries to all servers
func (rf *Raft) issueAppendEntries(hb bool) {
	if rf.debug {
		fmt.Printf("Server %d issues AppendEntries, HB: %v\n", rf.me, hb)
	}
	// Use curTerm for all requests
	var curTerm int
	rf.mu.Lock()
	curTerm = rf.currentTerm
	rf.mu.Unlock()
	for n := 0; n < len(rf.peers); n++ {
		if n == rf.me {
			continue
		}
		go rf.issueSingleAppendEntries(n, curTerm, hb)
	}
}

// Init states for the elected leader
func (rf *Raft) leaderInit() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i:=0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
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
	isLeader := false

	// Your code here (2B).
	isLeader = (rf.state == LEADER)
	term = rf.currentTerm
	if isLeader == true {
		index = len(rf.log)
		rf.log = append(rf.log, Entry{Command: command, Term: rf.currentTerm})
		if rf.debug {
			fmt.Printf("\n-----Start command %v on server %d (%v), index %d, term %d, rf_log %v-----\n", command, rf.me, rf.state, index, term, rf.log)
		}
		go rf.issueAppendEntries(false)
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off rf.debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.debug = false
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
	rf.commitChan = make(chan ApplyMsg)
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = FOLLOWER
	// Placeholder Entry at index 0
	rf.log = append(rf.log, Entry{Term: 0})
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.timeout = 0
	rf.debug = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.debug {
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
	}

	// Timer goroutine
	go func(){
		for {
			rf.mu.Lock()
			// if rf.debug {
			// 	fmt.Printf("Timer Log for Server %d: state %v\n", rf.me, rf.state)
			// }
			if rf.state == FOLLOWER {
				if rf.timeout == 1 && rf.votedFor == -1{
					if rf.debug {
						fmt.Printf("FOLLOWER %d Timeout: Become Candidate\n", rf.me)
					}
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
					time.Sleep(randTimeOut(500))
				}
				continue
			}
			if rf.state == CANDIDATE {
				if rf.timeout == 1 {
					if rf.debug {
						fmt.Printf("CANDIDATE %d Timeout: Become Candidate\n", rf.me)
					}
					rf.currentTerm += 1
					rf.timeout = 0
					rf.votedFor = rf.me
					rf.mu.Unlock()
					go rf.issueRequestVote()
				} else {
					rf.timeout = 1
					rf.mu.Unlock()
					time.Sleep(randTimeOut(500))
				}
				continue
			}
			if rf.state == LEADER {
				rf.timeout = 0
				rf.mu.Unlock()
				go rf.issueAppendEntries(true)
				time.Sleep(randTimeOut(200))
			}
		}
	}()

	// // Check if any server's log needs to be updated
	// go func(){
	// 	rf.mu.Lock()
	// 	defer rf.mu.Unlock()
	// 	for i, ele := range rf.nextIndex {
	// 		if len(rf.log)-1 >= ele {
	// 			if rf.debug {
	// 				fmt.Printf("Update Log from server %d to server %d in term %d\n", rf.me, i, rf.currentTerm)
	// 			}
	// 			rf.mu.Unlock()
	// 			go rf.issueSingleAppendEntries(i, rf.currentTerm, false)
	// 			rf.mu.Lock()
	// 		}
	// 	}
	// 	time.Sleep(200*time.Millisecond)
	// }()

	// Update leader commitIndex goroutine
	go func(){
		for {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				time.Sleep(300*time.Millisecond)
				continue
			}
			// Check matchIndex
			var m map[int]int
			m = make(map[int]int)
			for ele := rf.commitIndex+1; ele < len(rf.log); ele++ {
				if rf.log[ele].Term == rf.currentTerm {
					m[ele] = 0
				}
			}
			M := -1
			for key, _ := range m {
				for _, ele := range rf.matchIndex {
					if ele >= key {
						m[key] += 1
					}
				}
			}
			for key, value := range m {
				// Majority
				if value >= len(rf.peers)/2 {
					if key >= M {
						M = key
					}
				}
			}
			if rf.debug {
				fmt.Printf("***** Match Index Max Value: %d *****\n MatchIndex %v\n Map: %v\n", M, rf.matchIndex, m)
			}
			if M != -1 && M > rf.commitIndex{
				if rf.debug {
					fmt.Printf("!!! Leader commitIndex Update: from %d to %d !!!\n", rf.commitIndex, M)
				}
				// Apply leader's log to the state machine
				for i := rf.commitIndex+1; i <= M; i++ {
					command := rf.log[i].Command
					index := i
					rf.mu.Unlock()
					rf.commitChan <- ApplyMsg{Index: index, Command: command}
					rf.mu.Lock()
				}
				rf.commitIndex = M
			}
			rf.mu.Unlock()
			time.Sleep(200*time.Millisecond)
		}
	}()

	// ApplyMsg goroutine
	go func(){
		for command := range rf.commitChan{
			rf.mu.Lock()
			if rf.debug {
				fmt.Printf("##### Command Applied for server %d, Command %v, Term %d ####\n", rf.me, command, rf.currentTerm)
			}
			rf.mu.Unlock()
			applyCh <- command
		}
	}()
	return rf
}
