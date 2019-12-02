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

import (
	"labrpc"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

const (
	minElectionTimeout = 300
	maxElectionTimeout = 600
	heartbeatTimeout = 150
	followerRole       = 0
	candidateRole      = 1
	leaderRole         = 2
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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

	currentTerm int    // latest term server has seen
	votedFor    int    // candidate index that received vote in current term (or -1 if none)
	logs        []LogEntry // log entries

	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// leader states
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// custom fields
	role              int       // peer role: follower/candidate/leader
	electionTimestamp time.Time // timestamp of when the latest election timer was started
}

// LogEntry is a struct for log entries, used for both local logs and entries field in AppendEntries RPCs
type LogEntry struct {
	Command     interface{}
	TermCreated int // term when entry was received by leader (first index is 1)
}

// GetState return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	var term int
	var isLeader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.role == leaderRole
	rf.mu.Unlock()
	// DPrintf("%d is leader %t", rf.me, isLeader)
	return term, isLeader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("%d recv RequestVote from %d", rf.me, args.CandidateId)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogIdx := -1
	lastLogTerm := 0
	if len(rf.logs) > 0 {
		lastLogIdx = len(rf.logs)
		lastLogTerm = rf.logs[lastLogIdx-1].TermCreated
	}

	if args.Term >= rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm >= lastLogTerm && args.LastLogIndex >= lastLogIdx) {
		reply.Term = args.Term
		reply.VoteGranted = true

		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		go rf.startElectionTimer()
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

	DPrintf("%d reply to %d: %v", rf.me, args.CandidateId, reply)
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

type AppendEntryArgs struct {
	Term int	// leader’s term
	LeaderId int	// so follower can redirect clients
	PrevLogIndex int	// index of log entry immediately preceding new ones
	PrevLogTerm int	// term of prevLogIndex entry
	Entries []LogEntry	// log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int	// leader’s commitIndex
}

type AppendEntryReply struct {
	Term int	// currentTerm, for leader to update itself
	Success bool	// true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	// TODO: validation

	// convert to follower
	rf.mu.Lock()
	rf.role = followerRole
	rf.votedFor = -1
	rf.mu.Unlock()
	go rf.startElectionTimer()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// synchronous function
func (rf *Raft) handleAppendEntries(peerIdx int) {
	rf.mu.Lock()
	lastLogIdx := len(rf.logs)
	nextIdx := rf.nextIndex[peerIdx]
	sendEmptyRPC := lastLogIdx < nextIdx

	args := &AppendEntryArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		LeaderCommit: rf.commitIndex,
	}
	if !sendEmptyRPC {
		args.PrevLogIndex = nextIdx-1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].TermCreated
		args.Entries = rf.logs[nextIdx-1:]
	}
	rf.mu.Unlock()

	reply := &AppendEntryReply{}
	if ok := rf.sendAppendEntries(peerIdx, args, reply); ok {
		// if reply.Success {
		// 	rf.mu.Lock()
		// 	rf.nextIndex[peerIdx] = lastLogIdx + 1
		// 	rf.matchIndex[peerIdx] = lastLogIdx
		// 	rf.mu.Unlock()
		// } else {
		// 	hasOutdatedTerm := rf.handleReplyTerm(reply.Term)
		// 	if !hasOutdatedTerm { // AppendEntries fails because of log inconsistency, decrement nextIndex and retry
		// 		rf.mu.Lock()
		// 		rf.nextIndex[peerIdx]--
		// 		rf.mu.Unlock()
		// 		rf.handleAppendEntries(peerIdx) // retry
		// 	}
		// }
	} else {
		// DPrintf("%d sendAppendEntries to peer %d failed", rf.me, peerIdx)
	}
}

func (rf *Raft) peerAppendEntries() {
	for i := range rf.peers {
		if i == rf.me { // skip self
			continue
		}
		go rf.handleAppendEntries(i)
	}
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	rf.role = candidateRole // set role to candidate
	rf.votedFor = rf.me     // vote for self
	rf.currentTerm++	// increment currentTerm
	currentTerm := rf.currentTerm
	lastLogIdx := len(rf.logs)
	rf.mu.Unlock()

	go rf.startElectionTimer() // reset election timer

	// send RequestVote RPCs to all other servers
	lastLogTerm := 0
	if lastLogIdx > 0 {
		lastLogTerm = rf.logs[lastLogIdx-1].TermCreated
	}
	args := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIdx,
		LastLogTerm:  lastLogTerm,
	}

	DPrintf("%d becomes candidate, args %v", rf.me, args)

	voteCh := make(chan int)
	for i := range rf.peers {
		if i == rf.me { // skip self
			continue
		}
		go func(idx int) {
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(idx, args, reply); ok {
				rf.handleReplyTerm(reply.Term)
				if reply.VoteGranted {
					voteCh <- 1
				} else {
					voteCh <- 0
				}
			} else {
				DPrintf("%d sendRequestVote to peer %d failed", rf.me, idx)
				voteCh <- 0
			}
		}(i)
	}

	numVotes := 1 // voted for self
	numReply := 0
	majority := len(rf.peers)/2+1

	loop: for {
		select {
		case val := <-voteCh:
			numVotes += val
			numReply++
			if numReply >= majority-1 { // received reply for majority of peer voters, excluding itself
				break loop
			}
		default:
		}
	}

	rf.mu.Lock()
	isCandidate := rf.role == candidateRole
	rf.mu.Unlock()
	if numVotes >= majority && isCandidate {
		DPrintf("%d becomes leader, votes: %d", rf.me, numVotes)
		rf.becomeLeader()
	} else {
		rf.mu.Lock()
		rf.votedFor = -1
		rf.mu.Unlock()
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	rf.role = leaderRole
	lastLogIdx := len(rf.logs)
	rf.mu.Unlock()

	// (re)initialize nextIndex and matchIndex slices when elected
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIdx+1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}

	rf.startHeartbeatTimer()
}

func (rf *Raft) startElectionTimer() {
	rf.mu.Lock()
	rf.electionTimestamp = time.Now()
	rf.mu.Unlock()
	timeout := rand.Intn(maxElectionTimeout-minElectionTimeout) + maxElectionTimeout
	dur, _ := time.ParseDuration(strconv.Itoa(timeout) + "ms")
	time.Sleep(dur)

	rf.mu.Lock()
	hasVoted := rf.votedFor != -1
	rf.mu.Unlock()
	if int(time.Since(rf.electionTimestamp).Seconds()*1e3) >= timeout && !hasVoted {
		// timeout waiting for leader heartbeat and hasn't voted for other candidates, become candidate
		rf.becomeCandidate()
	}
}

func (rf *Raft) startHeartbeatTimer() {
	rf.mu.Lock()
	isLeader := rf.role == leaderRole
	rf.mu.Unlock()

	if !isLeader {
		return
	}

	rf.peerAppendEntries()

	dur, _ := time.ParseDuration(strconv.Itoa(heartbeatTimeout) + "ms")
	time.Sleep(dur)
	rf.startHeartbeatTimer()
}

// handleReplyTerm returns true if reply term is greater than rf.currentTerm
func (rf *Raft) handleReplyTerm(replyTerm int) (bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if replyTerm > rf.currentTerm {
		rf.currentTerm = replyTerm
		rf.role = followerRole
		return true
	}
	return false
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logs = []LogEntry{}

	go rf.startElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
