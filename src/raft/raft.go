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
	"sync"
	"sync/atomic"
	"time"

	"context"
	"math/rand"

	"log"

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
	TimeoutRPC                 int64 = 500

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
	statusLock sync.Mutex

	cancelFunc context.CancelFunc
}

// LogEntry for log structure
type LogEntry struct {
	Log  interface{}
	Term int
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
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.status == StatusFollower {
		rf.timeoutChan <- 0
	}
	reply.Term = rf.getTermSync()
	// don't grant, whatever it is
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		// turn to Follower, set voteFor = None, and continue
		// notice: here, the current Term has been set to the lastest term
		log.Printf("Server %v Term %v during vote turn to FL from %v newTerm %v\n", rf.me, rf.currentTerm, rf.status, args.Term)
		rf.convertToFollower(args.Term)
	}

	//log.Printf("Server %v: In term %v for candidate %v, res: %v\n", rf.me, rf.currentTerm, args.CandidateID, reply.VoteGranted)
	if rf.status == StatusLeader || rf.status == StatusCandidate {
		log.Printf("Server %v: In term %v reject vote from %v, as not a follower\n", rf.me, rf.currentTerm, args.CandidateID)
		reply.VoteGranted = false
		return
	}
	// Consider as a follower below
	if rf.votedFor == args.CandidateID {
		log.Printf("Server %v: In term %v vote for %v\n", rf.me, rf.currentTerm, rf.votedFor)
		reply.VoteGranted = true
		return
	}

	if rf.votedFor != VoteForNone { // already vote for another
		reply.VoteGranted = false
		log.Printf("Server %v Term %v reject, has voted for %v\n", rf.me, rf.currentTerm, rf.votedFor)
		return
	}

	// also consider this request as a HeartBeat

	// if self is a Follower and not voted yet
	if rf.currentTerm <= args.Term {
		if rf.lastApplied == InitialTerm {
			reply.VoteGranted = true
		} else {
			lastLogTerm := rf.log[rf.lastApplied].Term
			if lastLogTerm > args.LastLogTerm {
				reply.VoteGranted = false
			} else if lastLogTerm == args.LastLogTerm {
				if rf.lastApplied <= args.LastLogIndex {
					reply.VoteGranted = true
				} else {
					reply.VoteGranted = false
				}
			} else { // lostLogTerm < args.LastLogTerm
				reply.VoteGranted = true
			}
		}
	} else {
		reply.VoteGranted = false
	}
	rf.mu.Lock()
	if !rf.killed() && reply.VoteGranted && (rf.votedFor == VoteForNone || rf.votedFor == args.CandidateID) {
		rf.votedFor = args.CandidateID
	} else {
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
	log.Printf("Server %v: Term %v from server %d with Term %v....Voted %v\n", rf.me, rf.currentTerm, args.CandidateID, args.Term, reply.VoteGranted)
}

//AppendEntries: handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//Suppose rf could not be a leader; otherwise there are two leaders at the same time.

	//log.Printf("Received HB, term %v\n", rf.currentTerm)
	rf.mu.Lock()
	xterm := rf.currentTerm
	rf.mu.Unlock()
	if xterm > args.Term {
		reply.Term = xterm
		reply.Success = false
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
		if rf.currentTerm < args.Term { //continue the election
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
		rf.convertToFollower(args.Term)
		// acknowledge the leader
	}
	// as a follower
	if rf.currentTerm > args.Term { //received illegal request
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.convertToCandidate()
	} else {
		if len(args.Entries) == 0 { // HeartBeat
			// reset timeout
			rf.timeoutChan <- 0
			if rf.currentTerm < args.Term {
				rf.convertToFollower(args.Term)
			}
		} else {
			// Append Entry
			log.Println("Append Req")
			for _, e := range args.Entries {
				entry := &LogEntry{
					Term: e.Term,
					Log:  e.Log,
				}
				rf.log = append(rf.log, entry)
			}
		}
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
	//set a relative short timetout compared to Heartbeat
	rf.electionTimeout = time.Duration(rand.Int63n(TimeoutMilliSecElectionMax-
		TimeoutMilliSecElectionMin)+TimeoutMilliSecElectionMin) * time.Millisecond

	log.Printf("Server %v convert to condidate at term %v\n", rf.me, rf.currentTerm)
}

func (rf *Raft) convertToFollower(newTerm int) {
	rf.mu.Lock()
	xterm := rf.currentTerm
	rf.mu.Unlock()
	if newTerm < xterm {
		return
	}
	//log.Printf("Server %v convert to follower at term %v newTerm %v\n", rf.me, rf.currentTerm, newTerm)
	rf.mu.Lock()
	rf.status = StatusFollower
	rf.votedFor = VoteForNone
	rf.currentTerm = newTerm
	rf.mu.Unlock()
}

func (rf *Raft) convertToLeader() {
	if rf.killed() || rf.getStatusSync() != StatusCandidate {
		return
	}
	log.Printf("Server %v Term %v turn to leader", rf.me, rf.currentTerm)
	rf.mu.Lock()
	rf.status = StatusLeader
	rf.votedFor = VoteForNone
	rf.mu.Unlock()
	//rf.sendHeartBeat()
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = rf.lastApplied + 1
	}
	for idx := range rf.matchIndex {
		rf.matchIndex[idx] = rf.commitIndex + 1
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
		log.Printf("Can %v Term %v RPC to %v Timeout", rf.me, args.Term, server)
		reply.VoteGranted = false
		break
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, wg *sync.WaitGroup) bool {
	defer wg.Done()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	sts := rf.status

	if sts != StatusLeader || rf.killed() {
		isLeader = false
		return index, rf.currentTerm, isLeader
	}
	// Append locally
	rf.log = append(rf.log,
		&LogEntry{
			Term: rf.currentTerm,
			Log:  command})
	rf.lastApplied++
	index = rf.me
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
	log.Println("Kill")
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
			log.Printf("Server %v killed, HB quit", rf.me)
			return
		}
		sts := rf.getStatusSync()
		if sts == StatusFollower || sts == StatusCandidate {
			//log.Printf("Server %v receives HB", rf.me)
			select {
			case <-rf.timeoutChan:
				//log.Printf("Server %v: Recieved Heartbeat.", rf.me)
			case <-time.After(rf.timeout):
				sts = rf.getStatusSync()
				if sts == StatusFollower && rf.votedFor == VoteForNone {
					//log.Printf("Server %v Term %v HB timeout", rf.me, rf.currentTerm)
					rf.convertToCandidate()
				}
			}
		} else { // leader
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
	args := &RequestVoteArgs{
		Term:         cterm, //use original term rather than real time term
		CandidateID:  rf.me,
		LastLogIndex: rf.lastApplied,
		LastLogTerm:  rf.log[rf.lastApplied].Term,
	}
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
			log.Printf("Candidate %v get voter's higher term %v\n", rf.me, resp.Term)
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

	rf.mu.Lock()
	xterm := rf.currentTerm
	rf.mu.Unlock()
	if xterm != cterm { //timeout
		log.Printf("AAA Candidate %v Term %v Old Term %v", rf.me, xterm, cterm)
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
			log.Printf("BBB Candidate %v Term %v Status %v get votes %v", rf.me, rf.currentTerm, rf.status, tmp)
			electionChan <- ElectionSuccess
			rf.convertToLeader()
		} else {
			log.Printf("CCC Candidate %v Term %v Status %v get votes %v", rf.me, rf.currentTerm, rf.status, tmp)
			electionChan <- ElectionFail
		}
	}
}

// election details for candidate
func (rf *Raft) canOperation(ctx context.Context, cterm int) {
	//log.Printf("Server %v Term %v into can op", rf.me, rf.currentTerm)
	if rf.killed() {
		return
	}
	electionChan := make(chan int, 1)
	go rf.requestVotes(ctx, electionChan, cterm)
	//sync
	select {
	case electionSts := <-electionChan:
		//log.Printf("Server %d get result, status: %d\n", rf.me, electionSts)
		if electionSts == ElectionSuccess ||
			electionSts == ElectionTurnToFollower {
			log.Printf("Candidate %v Term %v quit", rf.me, cterm)
		} else if electionSts == ElectionFail { // else: failed in this term, retry
			log.Printf("Can %v cterm %v Failed", rf.me, cterm)
			rf.mu.Lock()
			rf.currentTerm++
			rf.randomElectionTimeout()
			rf.mu.Unlock()
			return
		}
		return
	case <-time.After(rf.electionTimeout):
		//close(electionChan)
		rf.mu.Lock()
		if rf.currentTerm != cterm {
			log.Printf("Can %v cterm %v nterm %v", rf.me, cterm, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		rf.currentTerm = cterm + 1
		rf.electionTimeout = rf.randomElectionTimeout()
		log.Printf("Server: %d Election timeout. nTerm: %v\n", rf.me, cterm+1)
	}
}

func (rf *Raft) randomElectionTimeout() time.Duration {
	return time.Duration(rand.Int63n(TimeoutMilliSecElectionMax-TimeoutMilliSecElectionMin)+
		TimeoutMilliSecElectionMin) * time.Millisecond
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	xterm := rf.currentTerm // because leader will maintain its leadership in one term,
	//so we use xterm in the whole namepsace
	rf.mu.Unlock()

	var wg sync.WaitGroup
	args := &AppendEntriesArgs{
		Term:         xterm,
		LeaderID:     rf.me,
		PreLogIndex:  rf.lastApplied,
		PreLogTerm:   rf.log[rf.lastApplied].Term,
		Entries:      []LogEntry{},
		LeaderCommit: rf.commitIndex,
	}
	replies := make([]AppendEntriesReply, len(rf.peers))

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		wg.Add(1)
		go rf.sendAppendEntries(idx, args, &replies[idx], &wg)
	}
	wg.Wait()
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		// outdated leader
		if replies[idx].Term > xterm {
			rf.convertToFollower(replies[idx].Term)
			log.Printf("Server %v xterm %v Receive Term higher HB.", rf.me, xterm)
			break
		}
	}
	//log.Printf("Leader %v Send HB term: %v\n", rf.me, rf.currentTerm)
}

// check as a leader
func (rf *Raft) checkLogEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != StatusLeader || rf.killed() {
		return
	}
	if rf.commitIndex < rf.lastApplied {

	}
}

// main controller
func (rf *Raft) startController(ctx context.Context) {
	HBCtx, HBFunc := context.WithCancel(ctx)
	CanCtx, CanFunc := context.WithCancel(ctx)
	go rf.checkHeartBeats(HBCtx)
	for {
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
			go rf.checkLogEntries()
			time.Sleep(time.Duration(TimeoutMilliSecSendHB/2) * time.Millisecond)
		} else { //Candidate
			//log.Printf("Can %v RV", rf.me)
			rf.canOperation(CanCtx, rf.getTermSync())
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
	rf.log = []*LogEntry{&LogEntry{Log: nil, Term: -1}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.timeout = time.Millisecond * time.Duration((rand.Int63n(TimeoutMilliSecMax-
		TimeoutMilliSecMin) + TimeoutMilliSecMin))

	//rf.lastReceivedAt = time.Now()
	rf.timeoutChan = make(chan int)
	log.Printf("Server %v Start...\n", rf.me)
	go rf.startController(ctx)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
