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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"context"
	"math/rand"

	"log"

	"bytes"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

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
type ServerStatus int32

const (
	StatusFollower  ServerStatus = 0 // follower
	StatusCandidate ServerStatus = 1 // candidate
	StatusLeader    ServerStatus = 2 // leader

	TimeoutMilliSecMin         int64 = 500 //checking heartbeats for followers
	TimeoutMilliSecMax         int64 = 1000
	TimeoutMilliSecSendHB      int64 = 200
	TimeoutMilliSecElectionMin int64 = 600
	TimeoutMilliSecElectionMax int64 = 1000
	TimeoutRPC                 int64 = 800
	TimeoutRPCAE               int64 = 1000

	HeartBeatCrycle int64 = 2
	InitialTerm     int   = 0

	VoteForNone int = -1

	ElectionSuccess        int = 0
	ElectionFail           int = 1
	ElectionTurnToFollower int = 2
)

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
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	status      ServerStatus
	currentTerm int
	votedFor    int
	log         []*LogEntry

	//timeout for HeartBeat messages
	timeoutChan     chan int
	timeout         time.Duration
	electionTimeout time.Duration

	// for all servers
	commitIndex int
	lastApplied int

	// for leaders only
	nextIndex  []int
	matchIndex []int

	//locks
	applyCh chan ApplyMsg
	//mu         sync.Mutex
	statusLock sync.Mutex

	cancelFunc context.CancelFunc
}

// LogEntry for log structure
type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.status == StatusLeader)
	//log.Printf("Server %v get state\n", rf.me)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(len(rf.log))
	for idx := 0; idx < len(rf.log); idx++ {
		var log = *rf.log[idx]
		e.Encode(log)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rb := bytes.NewBuffer(data)
	d := labgob.NewDecoder(rb)
	var (
		cterm    int
		logCount int
		voteFor  int
	)
	if d.Decode(&cterm) != nil || d.Decode(&voteFor) != nil ||
		d.Decode(&logCount) != nil {
		log.Panicf("Server %v Decode error", rf.me)
		return
	} else {
		rf.mu.Lock()
		rf.currentTerm = cterm
		rf.votedFor = voteFor
		log.Printf("Server %v read persist cterm %v", rf.me, cterm)
		rf.log = []*LogEntry{}

		for idx := 0; idx < logCount; idx++ {
			var e = &LogEntry{}
			d.Decode(e)
			rf.log = append(rf.log, e)
		}
		rf.mu.Unlock()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	reply.Term = rf.getTermSync()
	// don't grant, whatever it is
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		// turn to Follower, set voteFor = None, and continue
		// notice: here, the current Term has been set to the lastest term
		//log.Printf("Server %v Term %v during vote turn to FL from %v newTerm %v\n", rf.me, rf.currentTerm, rf.status, args.Term)
		rf.convertToFollower(args.Term)
	}

	//log.Printf("Server %v: In term %v for candidate %v, res: %v\n", rf.me, rf.currentTerm, args.CandidateID, reply.VoteGranted)
	if rf.status == StatusLeader || rf.status == StatusCandidate {
		//log.Printf("Server %v: In term %v reject vote from %v, as not a follower\n", rf.me, rf.currentTerm, args.CandidateID)
		reply.VoteGranted = false
		return
	}
	// Consider as a follower below
	if rf.votedFor == args.CandidateID {
		log.Printf("Server %v: In term %v vote for %v\n", rf.me, rf.currentTerm, rf.votedFor)
		reply.VoteGranted = true
		rf.timeoutChan <- 1 //reset timeout
		return
	}

	if rf.votedFor != VoteForNone { // already vote for another
		reply.VoteGranted = false
		log.Printf("Server %v Term %v reject, has voted for %v\n", rf.me, rf.currentTerm, rf.votedFor)
		return
	}

	// also consider this request as a HeartBeat

	// if self is a Follower and not voted yet
	rf.mu.Lock()
	if rf.currentTerm <= args.Term {
		// restriction in 5.4
		lastIdx := len(rf.log) - 1
		if rf.log[lastIdx].Term > args.LastLogTerm {
			reply.VoteGranted = false
		} else if rf.log[lastIdx].Term < args.LastLogTerm {
			reply.VoteGranted = true
		} else {
			if lastIdx <= args.LastLogIndex {
				reply.VoteGranted = true
			} else {
				reply.VoteGranted = false
			}
		}
		//log.Printf("FL(%v) preIdx %v, preTerm %v args.PreIdx %v args.PreTerm %v, Vote %v", rf.me, lastIdx, rf.log[lastIdx].Term, args.LastLogIndex, args.LastLogTerm, reply.VoteGranted)
	} else {
		reply.VoteGranted = false
	}
	if !rf.killed() && reply.VoteGranted && (rf.votedFor == VoteForNone || rf.votedFor == args.CandidateID) {
		rf.timeoutChan <- 1 //reset timer
		rf.votedFor = args.CandidateID
		rf.persist()
	} else {
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
	//log.Printf("Server %v: Term %v from server %d with Term %v....Voted %v\n", rf.me, rf.currentTerm, args.CandidateID, args.Term, reply.VoteGranted)
}

//AppendEntries: handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//Suppose rf could not be a leader; otherwise there are two leaders at the same time.

	//log.Printf("Received HB, term %v\n", rf.currentTerm)
	xterm := rf.currentTerm
	if xterm > args.Term {
		reply.Term = xterm
		reply.Success = false
		log.Printf("AAAAAAA FL(%v) Term %v args.Term %v", rf.me, rf.currentTerm, args.Term)
		rf.convertToCandidate()
		return
	} else {
		rf.convertToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	if rf.status == StatusLeader {
		reply.Success = false
		return
	}

	if rf.status == StatusCandidate {
		if rf.currentTerm > args.Term { //continue the election
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
		rf.convertToFollower(args.Term)
		// acknowledge the leader
	}
	// as a follower
	if rf.getTermSync() > args.Term { //received illegal request
		reply.Term = rf.currentTerm
		reply.Success = false
		log.Printf("FL %v AAAAAAA", rf.me)
		rf.convertToCandidate()
	} else {
		rf.timeoutChan <- 1 // reset timeout

		if rf.getTermSync() < args.Term {
			rf.convertToFollower(args.Term)
			return
		}
		rf.mu.Lock()
		logCnt := len(rf.log)
		if args.PreLogIndex >= logCnt {
			reply.ConflictIndex = len(rf.log)
			reply.ConflictTerm = -1
			rf.mu.Unlock()
			//log.Printf("FL %v Term %v, preLogIdx: %v, local cnt: %v\n	AE Entry: %v, out of index",
			//	rf.me, rf.currentTerm, args.PreLogIndex, logCnt, args.Entries)
			reply.Success = false
			return
		}

		if rf.log[args.PreLogIndex].Term != args.PreLogTerm {
			//log.Printf("FL %v Term %v, Term issue		preLogTerm : %v, args.preTerm: %v", rf.me, rf.currentTerm,
			//	rf.log[args.PreLogIndex].Term, args.PreLogTerm)
			reply.ConflictTerm = rf.log[args.PreLogIndex].Term
			//rf.printLogs()
			//search for the first log of this term
			var idx = 0
			for ; idx < len(rf.log); idx++ {
				if rf.log[idx].Term == reply.ConflictTerm {
					break
				}
			}
			reply.ConflictIndex = idx
			reply.Success = false
			rf.log = rf.log[:args.PreLogIndex]
			rf.mu.Unlock()
			return
		}

		if len(args.Entries) == 0 { // HeartBeat

			reply.Success = true
		} else {
			// Append Entry
			//log.Printf("FL %v Term %v log size before %v", rf.me, rf.currentTerm, len(rf.log))
			startIdxOri := args.PreLogIndex + 1
			startIdxNew := 0
			for startIdxOri < len(rf.log) && startIdxNew < len(args.Entries) {
				if rf.log[startIdxOri].Term != args.Entries[startIdxNew].Term {
					break
				}
				startIdxOri++
				startIdxNew++
			}
			if startIdxNew < len(args.Entries) {
				rf.log = rf.log[:startIdxOri]
				for ; startIdxNew < len(args.Entries); startIdxNew++ {
					var e = args.Entries[startIdxNew]
					entry := &LogEntry{
						Term:    e.Term,
						Command: e.Command,
					}
					rf.log = append(rf.log, entry)
				}
			}

			//log.Printf("FL %v Term %v success, args: %v, log size after: %v", rf.me, rf.currentTerm, args.PreLogIndex,
			//	len(rf.log))
			reply.Success = true
			rf.printLogs()
		}
		// #5 in Receiver impl
		//oldCidx := rf.commitIndex
		if args.LeaderCommit > rf.commitIndex {
			lastIndex := len(rf.log) - 1
			if lastIndex < args.LeaderCommit {
				rf.commitIndex = lastIndex
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			//log.Printf("FL %v update commitIdx(%v) lastIndex(%v) args.Commit(%v) ", rf.me, oldCidx, lastIndex, args.LeaderCommit)
		}
		//log.Printf("FL %v commitIdx %v lastApply %v args.commitIdx %v args.logs %v", rf.me, rf.commitIndex, rf.lastApplied, args.LeaderCommit, len(args.Entries))
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			//log.Printf("FL(%v) apply log(%v)", rf.me, rf.lastApplied)
			//log.Printf("Server %v apply %v", rf.me, rf.lastApplied)
			rf.applyCh <- ApplyMsg{
				Command:      rf.log[rf.lastApplied].Command,
				CommandValid: true,
				CommandIndex: rf.lastApplied,
			}
		}
		rf.mu.Unlock()
		rf.persist()
	}
}

func (rf *Raft) convertToCandidate() {
	if rf.status == StatusCandidate || rf.killed() {
		return
	}
	rf.mu.Lock()
	rf.currentTerm++
	rf.status = StatusCandidate
	rf.votedFor = rf.me
	rf.mu.Unlock()
	rf.persist()
	//set a relative short timetout compared to Heartbeat
	rf.electionTimeout = time.Duration(rand.Int63n(TimeoutMilliSecElectionMax-
		TimeoutMilliSecElectionMin)+TimeoutMilliSecElectionMin) * time.Millisecond

	//log.Printf("Server %v convert to condidate at term %v\n", rf.me, rf.currentTerm)
}

func (rf *Raft) convertToFollower(newTerm int) {
	xterm := rf.currentTerm
	if newTerm < xterm {
		return
	}
	//log.Printf("Server %v convert to follower at term %v newTerm %v\n", rf.me, rf.currentTerm, newTerm)
	rf.mu.Lock()
	rf.timeout = rf.randomElectionTimeout()
	rf.status = StatusFollower
	rf.votedFor = VoteForNone
	rf.currentTerm = newTerm
	rf.mu.Unlock()
	rf.persist()
}

func (rf *Raft) convertToLeader() {
	if rf.killed() || rf.getStatusSync() != StatusCandidate {
		return
	}
	log.Printf("Server %v Term %v turn to leader", rf.me, rf.currentTerm)
	rf.mu.Lock()
	rf.status = StatusLeader
	rf.votedFor = VoteForNone
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = len(rf.log)
	}
	for idx := range rf.matchIndex {
		rf.matchIndex[idx] = 0
	}
	rf.mu.Unlock()
	rf.sendHeartBeat()
	rf.persist()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, toChan chan bool) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	toChan <- ok
	return ok
}

func (rf *Raft) sendRequestVoteWrapper(server int, args *RequestVoteArgs, reply *RequestVoteReply, wg *sync.WaitGroup) bool {
	defer wg.Done()
	ok := false
	toChan := make(chan bool, 1)
	go rf.sendRequestVote(server, args, reply, toChan)
	select {
	case ok = <-toChan:
		return ok
	case <-time.After(time.Duration(TimeoutRPC) * time.Millisecond):
		//log.Printf("Can %v Term %v RPC to %v Timeout", rf.me, args.Term, server)
		reply.VoteGranted = false
		break
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, toChan chan bool) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	toChan <- ok
	return ok
}

func (rf *Raft) sendAppendEntriesWrapper(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := false
	toChan := make(chan bool, 1)
	for rf.status == StatusLeader {
		go rf.sendAppendEntries(server, args, reply, toChan)
		select {
		case ok = <-toChan:
			if ok {
				return ok
			}
			continue
		case <-time.After(time.Duration(TimeoutRPCAE) * time.Millisecond):
			if len(args.Entries) == 0 { // since HB is routinely run, does not have to run it indefinitely
				return ok
			}
			log.Printf("Leader Append to FL %v timeout, preLogIdx: %v", server, args.PreLogIndex)
			continue
		}
	}
	return ok
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

	if rf.getStatusSync() != StatusLeader || rf.killed() {
		isLeader = false
		return index, rf.currentTerm, isLeader
	}
	// Append locally
	rf.mu.Lock()
	rf.log = append(rf.log,
		&LogEntry{
			Term:    rf.currentTerm,
			Command: command})
	index = len(rf.log) - 1
	rf.mu.Unlock()
	rf.persist()
	rf.printLogs()
	term = rf.currentTerm
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	//log.Println("Kill")
	//rf.cancelFunc()
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// for Followers/Candidates
func (rf *Raft) checkHeartBeats(ctx context.Context) {
	for {
		if rf.killed() {
			//log.Printf("Server %v killed, HB sleeped", rf.me)
			return
		}
		sts := rf.status
		if sts == StatusFollower || sts == StatusCandidate {
			//log.Printf("Server %v receives HB", rf.me)
			select {
			case <-rf.timeoutChan:
				//log.Printf("Server %v: Term %v Status %v Recieved HB", rf.me, rf.currentTerm, rf.status)
			case <-time.After(rf.timeout):
				sts = rf.getStatusSync()
				if sts == StatusFollower && rf.votedFor == VoteForNone && !rf.killed() {
					//log.Printf("Server %v Term %v HB timeout", rf.me, rf.currentTerm)
					rf.convertToCandidate()
				} else {
				}
			}
		} else { // leader
			//log.Printf("foo")
			time.Sleep(time.Duration(TimeoutMilliSecSendHB/2) * time.Millisecond)
		}
	}
}

func (rf *Raft) getStatusSync() ServerStatus {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status
}

// broadcast for voting
func (rf *Raft) requestVotes(ctx context.Context, electionChan chan int, cterm int) {
	if rf.getStatusSync() != StatusCandidate {
		//log.Println("quit")
		electionChan <- ElectionTurnToFollower
		return
	}
	rf.mu.Lock()
	lastIdx := len(rf.log) - 1
	args := &RequestVoteArgs{
		Term:         cterm, //use original term rather than real time term
		CandidateID:  rf.me,
		LastLogIndex: lastIdx,
		LastLogTerm:  rf.log[lastIdx].Term,
	}
	rf.mu.Unlock()
	tol := len(rf.peers)
	replies := make([]RequestVoteReply, tol)
	wg := &sync.WaitGroup{}
	wg.Add(tol - 1)
	for idx := range rf.peers {
		if idx == rf.me {
			replies[idx].Term = cterm
			continue
		}
		replies[idx].VoteGranted = false
		//wg.Add(1)
		go rf.sendRequestVoteWrapper(idx, args, &replies[idx], wg)
	}
	wg.Wait()
	// check
	voteCnt := 0
	tmp := []int{}
	for idx, resp := range replies {
		if resp.Term > cterm {
			//log.Printf("Candidate %v get voter's higher term %v\n", rf.me, resp.Term)
			rf.convertToFollower(resp.Term)
			electionChan <- ElectionTurnToFollower
			return
		}
		if idx == rf.me || resp.VoteGranted {
			voteCnt++
			tmp = append(tmp, idx)
			continue
		}
	}

	xterm := rf.currentTerm
	if xterm != cterm { //timeout
		//log.Printf("AAA Candidate %v Term %v Old Term %v", rf.me, xterm, cterm)
		//
	} else {
		//log.Printf("Server %v election finished.\n", rf.me)
		// it has received a HB message from the current leader
		if rf.status != StatusCandidate {
			//log.Printf("DDD Candidate %v Term %v Old Term %v Status %v get votes %v", rf.me, rf.currentTerm, cterm, rf.status, tmp)
			electionChan <- ElectionTurnToFollower
			return
		}
		if voteCnt > len(rf.peers)/2 {
			//log.Printf("BBB Candidate %v Term %v Status %v get votes %v", rf.me, rf.currentTerm, rf.status, tmp)
			electionChan <- ElectionSuccess
			rf.convertToLeader()
		} else { // not enough vote
			//log.Printf("CCC Candidate %v Term %v Status %v get votes %v", rf.me, rf.currentTerm, rf.status, tmp)
			electionChan <- ElectionFail
		}
	}
}

// election details for candidate
func (rf *Raft) canOperation(ctx context.Context, cterm int) {
	//log.Printf("Server %v Term %v into can op", rf.me, rf.currentTerm)

	electionChan := make(chan int, 1)
	go rf.requestVotes(ctx, electionChan, cterm)
	//sync
	select {
	case electionSts := <-electionChan:
		//log.Printf("Server %d get result, status: %d\n", rf.me, electionSts)
		if electionSts == ElectionSuccess ||
			electionSts == ElectionTurnToFollower {
			//log.Printf("Candidate %v Term %v quit", rf.me, cterm)
		} else if electionSts == ElectionFail { // else: failed in this term, retry
			log.Printf("Can %v cterm %v Failed", rf.me, cterm)
			rf.mu.Lock()
			rf.currentTerm++
			rf.electionTimeout = rf.randomElectionTimeout()
			rf.mu.Unlock()
			time.Sleep(time.Duration(rf.electionTimeout/2) * time.Millisecond)
			return
		}
		return
	case <-time.After(rf.electionTimeout):
		//close(electionChan)
		rf.mu.Lock()
		if rf.currentTerm != cterm {
			//log.Printf("Can %v cterm %v nterm %v", rf.me, cterm, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		rf.currentTerm = cterm + 1
		rf.electionTimeout = rf.randomElectionTimeout()
		//log.Printf("Server: %d Election timeout. nTerm: %v\n", rf.me, cterm+1)
		rf.mu.Unlock()
	}
}

func (rf *Raft) randomElectionTimeout() time.Duration {
	return time.Duration(rand.Int63n(TimeoutMilliSecElectionMax-TimeoutMilliSecElectionMin)+
		TimeoutMilliSecElectionMin) * time.Millisecond
}

func (rf *Raft) consistencyCheckFor(server, cterm int, wg *sync.WaitGroup) {
	defer wg.Done()
	rf.mu.Lock()
	ni := rf.nextIndex[server]
	preLogIdx := ni - 1
	preLogTerm := -1
	var entries []*LogEntry
	if ni >= len(rf.log) {
		entries = nil
	} else {
		entries = rf.log[ni:]
		//log.Printf("For FL(%v) ni %v len %v", server, ni, len(entries))
	}
	if preLogIdx < len(rf.log) {
		preLogTerm = rf.log[preLogIdx].Term
	}

	args := &AppendEntriesArgs{
		Term:         cterm,
		LeaderID:     rf.me,
		PreLogIndex:  preLogIdx,
		PreLogTerm:   preLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()

	toChan := make(chan bool, 1)
	rf.sendAppendEntries(server, args, reply, toChan)
	<-toChan
	if reply.Term > cterm {
		rf.convertToFollower(reply.Term)
		return
	}
	if rf.status != StatusLeader || cterm != reply.Term {
		return
	}
	rf.mu.Lock()
	if reply.Success {

		if preLogIdx+1 == rf.nextIndex[server] {
			rf.nextIndex[server] = ni + len(entries)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}

		oriCommitIndex := rf.commitIndex
		for i := rf.commitIndex + 1; i < len(rf.log); i++ {
			if rf.log[i].Term == rf.currentTerm { // this is very important
				matchCnt := 1
				for idx := range rf.peers {
					if idx == rf.me {
						continue
					}
					if rf.matchIndex[idx] >= i {
						matchCnt++
					}
				}
				if matchCnt > len(rf.peers)/2 {
					rf.commitIndex = i
				}
			}
		}
		if rf.commitIndex != oriCommitIndex {
			log.Printf("Leader update commit(%v) to %v", oriCommitIndex, rf.commitIndex)
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				//log.Printf("Server %v apply %v", rf.me, rf.lastApplied)
				rf.applyCh <- ApplyMsg{
					Command:      rf.log[rf.lastApplied].Command,
					CommandValid: true,
					CommandIndex: rf.lastApplied,
				}
			}
			rf.persist()
		}
		//*tag = true
	} else {
		if ni > 1 {
			// idx := reply.ConflictIndex
			// if reply.ConflictTerm != -1 {
			// 	for i := 0; i < len(rf.log); i++ {
			// 		if rf.log[i].Term == reply.ConflictTerm {
			// 			for i < len(rf.log) && rf.log[i].Term == reply.ConflictTerm {
			// 				i++
			// 			}
			// 			idx = i
			// 			break
			// 		}
			// 	}
			// }
			// rf.nextIndex[server] = idx
			rf.nextIndex[server] = ni - 1
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartBeat() {
	var wg sync.WaitGroup
	cterm := rf.currentTerm
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		wg.Add(1)
		go rf.consistencyCheckFor(idx, cterm, &wg)
	}
	wg.Wait()
}

func (rf *Raft) applyNewLog() {
	for {
		if rf.killed() {
			return
		}
		time.Sleep(time.Duration(200) * time.Millisecond)
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			//log.Printf("Server %v apply %v", rf.me, rf.lastApplied)
			rf.applyCh <- ApplyMsg{
				Command:      rf.log[rf.lastApplied].Command,
				CommandValid: true,
				CommandIndex: rf.lastApplied,
			}
		}
		rf.mu.Unlock()
	}
}

// main controller
func (rf *Raft) startController(ctx context.Context) {
	HBCtx, HBFunc := context.WithCancel(ctx)
	CanCtx, CanFunc := context.WithCancel(ctx)
	go rf.checkHeartBeats(HBCtx)
	go rf.applyNewLog()
	for !rf.killed() {
		select {
		case <-ctx.Done():
			// let sub thread quit
			HBFunc()
			CanFunc()
			return
		default:
		}
		// TODO: routine check
		sts := rf.getStatusSync()
		if sts == StatusFollower {

		} else if sts == StatusLeader {
			// check variable and send HB  /asyncronize
			go rf.sendHeartBeat()
			time.Sleep(time.Duration(TimeoutMilliSecSendHB/3) * time.Millisecond)
		} else { //Candidate
			//log.Printf("Can %v RV", rf.me)
			rf.canOperation(CanCtx, rf.currentTerm)
		}
	}
}

func (rf *Raft) getTermSync() int {
	rf.mu.Lock()
	xterm := rf.currentTerm
	defer rf.mu.Unlock()
	return xterm
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
	rf.currentTerm = InitialTerm
	rf.status = StatusFollower
	ctx, cancel := context.WithCancel(context.Background())
	rf.cancelFunc = cancel
	rf.votedFor = VoteForNone
	rf.log = []*LogEntry{&LogEntry{Command: nil, Term: -1}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.timeout = time.Millisecond * time.Duration((rand.Int63n(TimeoutMilliSecMax-
		TimeoutMilliSecMin) + TimeoutMilliSecMin))

	//rf.lastReceivedAt = time.Now()
	rf.timeoutChan = make(chan int)
	//log.Printf("Server %v Start...\n", rf.me)
	rf.readPersist(persister.ReadRaftState())
	go rf.startController(ctx)
	// initialize from state persisted before a crash
	return rf
}

func (rf *Raft) printLogs() {
	logstr := ""
	for id, e := range rf.log {
		if id == 0 {
			continue
		}
		s := fmt.Sprintf("%v ", e.Command)
		logstr += s
	}
	// tag := "Follower"
	// if rf.status == StatusLeader {
	// 	tag = "Leader"
	// }
	// log.Printf("%v %v, lastApply(%v), commitIdx(%v), log: %v", tag, rf.me, rf.lastApplied, rf.commitIndex, logstr)
}
