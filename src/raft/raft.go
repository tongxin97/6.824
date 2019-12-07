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
	minElectionTimeout = 200
	maxElectionTimeout = 300
	heartbeatTimeout   = 100
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

	currentTerm int        // latest term server has seen
	votedFor    int        // candidate index that received vote in current term (or -1 if none)
	logs        []LogEntry // log entries

	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// leader states
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// custom fields
	applyCh           chan ApplyMsg
	role              int       // peer role: follower/candidate/leader
	electionTimestamp time.Time // timestamp of when the latest election timer was started
	numMajority       int       // number of majority peers = len(rf.peers)/2 + 1, including the current server
}

// LogEntry is a struct for log entries, used for both local logs and entries field in AppendEntries RPCs
type LogEntry struct {
	Command     interface{}
	TermCreated int // term when entry was received by leader (first index is 1)
	LogIndex    int
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// synchronous function
func (rf *Raft) handleAppendEntries(peerIdx int) {
	rf.mu.Lock()
	lastLogIdx := len(rf.logs) - 1
	nextIdx := rf.nextIndex[peerIdx]
	sendEmptyRPC := lastLogIdx < nextIdx

	args := &AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	if !sendEmptyRPC {
		args.PrevLogIndex = nextIdx - 1
		args.PrevLogTerm = 0
		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].TermCreated
		}
		args.Entries = rf.logs[nextIdx:]
	}

	rf.mu.Unlock()

	/*
		If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
			- If successful: update nextIndex and matchIndex for follower (§5.3)
			- If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
	*/
	reply := &AppendEntryReply{}
	if ok := rf.sendAppendEntries(peerIdx, args, reply); ok {
		if reply.Success {
			rf.mu.Lock()
			rf.nextIndex[peerIdx] = lastLogIdx + 1
			rf.matchIndex[peerIdx] = lastLogIdx
			rf.mu.Unlock()
			// DPrintf("peer %d matchIndex %d", peerIdx, lastLogIdx)
		} else {
			convertedToFollower := rf.handleReplyTerm(reply.Term, -1)
			if !convertedToFollower { // AppendEntries fails because of log inconsistency, decrement nextIndex and retry
				rf.mu.Lock()
				rf.nextIndex[peerIdx]--
				rf.mu.Unlock()
				rf.handleAppendEntries(peerIdx) // retry
			}
		}
		if len(args.Entries) > 0 {
			DPrintf("%d sendAppendEntries %v to peer %d", rf.me, args, peerIdx)
		}
	} else {
		DPrintf("%d sendAppendEntries to peer %d failed", rf.me, peerIdx)
	}
}

func (rf *Raft) increaseCommitIndex() {
	/*
		If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
	*/
	rf.mu.Lock()
	N := rf.commitIndex + 1

	count := 0
	for _, matchIdx := range rf.matchIndex {
		if matchIdx >= N {
			count++
		}
	}

	if count >= rf.numMajority-1 && N < len(rf.logs) && rf.logs[N].TermCreated == rf.currentTerm {
		// send each new committed msg to rf.applyCh
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			CommandIndex: N,
			Command:      rf.logs[N].Command,
		}
		// increment commitIndex
		rf.commitIndex = N
	}
	rf.mu.Unlock()
}

func (rf *Raft) peerAppendEntries() {
	var wg sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me { // skip self
			continue
		}
		wg.Add(1)
		go func(i int) {
			rf.handleAppendEntries(i)
			wg.Done()
		}(i)
	}
	wg.Wait()
	rf.increaseCommitIndex()
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	rf.role = candidateRole // set role to candidate
	rf.votedFor = rf.me     // vote for self
	rf.currentTerm++        // increment currentTerm
	currentTerm := rf.currentTerm
	lastLogIdx := len(rf.logs) - 1
	rf.mu.Unlock()

	go rf.startElectionTimer() // reset election timer

	// send RequestVote RPCs to all other servers
	lastLogTerm := 0
	if lastLogIdx > 0 {
		lastLogTerm = rf.logs[lastLogIdx].TermCreated
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
		loop:
			for {
				if ok := rf.sendRequestVote(idx, args, reply); ok {
					if rf.handleReplyTerm(reply.Term, -1) { // if converted to follower, return
						return
					}
					if reply.VoteGranted {
						voteCh <- 1
					} else {
						voteCh <- 0
					}
					break loop
					// } else {
					// 	DPrintf("%d sendRequestVote to peer %d failed", rf.me, idx)
				}
			}
		}(i)
	}

	numVotes := 1 // voted for self
	numReply := 0
loop:
	for {
		select {
		case val := <-voteCh:
			numVotes += val
			numReply++
			if numReply >= rf.numMajority-1 { // received reply for majority of peer voters, excluding itself
				break loop
			}
		default:
		}
	}

	rf.mu.Lock()
	isCandidate := rf.role == candidateRole
	rf.mu.Unlock()
	if numVotes >= rf.numMajority && isCandidate {
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
	lastLogIdx := len(rf.logs) - 1
	rf.mu.Unlock()

	// (re)initialize nextIndex and matchIndex slices when elected
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIdx + 1
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

func (rf *Raft) handleReplyTerm(replyTerm int, voteFor int) (convertedToFollower bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if replyTerm > rf.currentTerm {
		rf.currentTerm = replyTerm
		rf.role = followerRole
		rf.votedFor = voteFor
		convertedToFollower = true
	}
	convertedToFollower = false
	return
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
	rf.mu.Lock()
	index = len(rf.logs)
	term = rf.currentTerm
	isLeader = rf.role == leaderRole

	if isLeader {
		log := LogEntry{
			Command:     command,
			TermCreated: rf.currentTerm,
			LogIndex:    index,
		}
		rf.logs = append(rf.logs, log)
	}
	rf.mu.Unlock()

	if isLeader {
		rf.peerAppendEntries()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
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
	rf.logs = []LogEntry{{nil, 0, 0}}

	rf.applyCh = applyCh
	rf.numMajority = len(peers)/2 + 1

	go rf.startElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
