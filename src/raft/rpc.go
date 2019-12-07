package raft

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

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term || // invalid candidate
		(rf.currentTerm == args.Term && rf.votedFor != -1) { // the server has voted in this term
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	// args.Term is higher, convert to follower and reset election timer
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.role = followerRole
		go rf.startElectionTimer()
	}

	lastLogIdx, lastLogTerm := -1, 0
	if len(rf.logs) > 1 {
		lastLogIdx, lastLogTerm = len(rf.logs)-1, rf.logs[lastLogIdx].TermCreated
	}
	if args.LastLogTerm >= lastLogTerm && args.LastLogIndex >= lastLogIdx {
		reply.Term, reply.VoteGranted = args.Term, true
		rf.votedFor = args.CandidateId
	} else {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
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
	// DPrintf("%d RequestVote to %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntryArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntryReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	if rf.handleReplyTerm(reply.Term, -1) {
		go rf.startElectionTimer()
		return
	}

	rf.mu.Lock()
	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= 1 {
		if prevLog := rf.logs[args.PrevLogIndex]; prevLog.TermCreated != args.PrevLogTerm {
			reply.Term, reply.Success = rf.currentTerm, false
			return
		}
	}

	/*
		If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	*/
	curLogIdx := args.PrevLogIndex + 1
	for i, log := range args.Entries {
		i += curLogIdx
		if i < len(rf.logs) && rf.logs[i].TermCreated != args.Term {
			rf.logs = rf.logs[:i]
		}
		// Append any new entries not already in the log
		rf.logs = append(rf.logs, log)
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	var newCommitIdx int
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.logs)-1 {
			newCommitIdx = args.LeaderCommit
		} else {
			newCommitIdx = len(rf.logs) - 1
		}
	}

	// Send newly committed logs to rf.applyCh
	for i := rf.commitIndex + 1; i <= newCommitIdx; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			CommandIndex: i,
			Command:      rf.logs[i].Command,
		}
	}
	rf.commitIndex = newCommitIdx

	reply.Term, reply.Success = rf.currentTerm, true

	if len(args.Entries) > 0 {
		DPrintf("AppendEntries from %d, reply: %v, logs: %v", args.LeaderId, reply, rf.logs)
	}
	rf.mu.Unlock()
}
