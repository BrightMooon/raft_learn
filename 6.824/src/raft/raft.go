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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int
const(
	Follower State=iota
	Candidate 
	Leader
)

type Log struct{
	Term int
	Command interface{} //状态机要执行的命令
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
	state State
	currentTerm int
	votedFor int
	log  []Log


	commitIndex int
	lastApplied int

	nextIndex []int  //对于每个服务器，记录需要发给他的下一个日志条目的索引
	matchIndex []int //对于每个服务器，记录已经复制到该服务器的日志的最高索引值 init=0

	applyCh  chan ApplyMsg
	// handle rpc
	voteCh chan bool
	appendLogCh chan bool

	heatbeatTime time.Duration
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term=rf.currentTerm
	isleader=(rf.state==Leader)
	return term, isleader
}

//---------raft 启动关闭-------------
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		electionTime :=time.Duration(150+rand.Intn(200)) * time.Millisecond
		rf.mu.Lock()
		state:=rf.state
		rf.mu.Unlock()
		switch state{
			case Follower,Candidate:
				select{
				case <- rf.voteCh:
					//DPrintf("ticker-rf.voteCh len=%d",len(rf.voteCh))
				case <-rf.appendLogCh:
					//DPrintf("ticker-rf.appendLogCh len=%d",len(rf.appendLogCh))
				case<-time.After(electionTime):
					rf.mu.Lock()
					rf.beCandidate()
					rf.mu.Unlock()
				}
			case Leader:
				rf.startAppendLog()
				time.Sleep(rf.heatbeatTime)
		}
	}
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
	rf.state=Follower
	rf.currentTerm=0
	rf.votedFor=-1
	rf.log=make([]Log,0)
	rf.commitIndex=0
	rf.lastApplied=0
	rf.nextIndex=make([]int,len(peers))
	rf.matchIndex=make([]int,len(peers))
	rf.heatbeatTime=time.Duration(100)*time.Millisecond
	rf.applyCh=applyCh
	rf.voteCh=make(chan bool,1)
	rf.appendLogCh=make(chan bool,1)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}


//---------partA-选举部分--------------------

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int  //候选人任期号
	CandidateId int
	LastLogIndex int  //候选人最新日志条目的索引
	LastLogTerm int  //候选人最新日志条目对应的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int  
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (args.Term>rf.currentTerm){
		rf.beFollower(args.Term)
		send(rf.voteCh)
	}
	success:=false
	if (args.Term<rf.currentTerm){
		return
	}else if rf.votedFor!=-1 && rf.votedFor!=args.CandidateId{
		return
	}else if args.LastLogTerm<rf.getLastLogTerm(){
		return
	}else if args.LastLogTerm==rf.getLastLogTerm() && args.LastLogIndex<rf.getLastLogIndex(){
		return
	}else{
		rf.votedFor=args.CandidateId
		success=true
		rf.state=Follower
		send(rf.voteCh)
	}
	reply.Term=rf.currentTerm
	reply.VoteGranted=success
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

func  (rf *Raft)beCandidate(){
	rf.state=Candidate
	rf.currentTerm++
	rf.votedFor=rf.me
	go rf.startElection()
}

func (rf *Raft)beFollower(term int){
	rf.state=Follower
	rf.votedFor=-1
	rf.currentTerm=term
}

func (rf *Raft)beLeader(){
	if rf.state!=Candidate{
		return
	}
	rf.state=Leader
	rf.nextIndex=make([]int,len(rf.peers))
	rf.matchIndex=make([]int,len(rf.peers))
	for i:=0;i<len(rf.nextIndex);i++{
		rf.nextIndex[i]=rf.getLastLogIndex()+1
	}
}

func (rf *Raft) startElection(){
	rf.mu.Lock()
	args:=RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm: rf.getLastLogTerm(),
	}
	rf.mu.Unlock()
	var votes int32=1
	for i:=0;i<len(rf.peers);i++{
		if i==rf.me{
			continue
		}
		go func(nodeIndex int){
			reply:=&RequestVoteReply{}
			response:=rf.sendRequestVote(nodeIndex,&args, reply)
			if response{
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term>rf.currentTerm{
					rf.beFollower(reply.Term)
					//这个send是干什么用的，voteCh怎么用
					send(rf.voteCh)
					return
				}
				if rf.state!=Candidate{
					return
				}
				if reply.VoteGranted{
					atomic.AddInt32(&votes,1)
				}
				if atomic.LoadInt32(&votes)>int32(len(rf.peers)/2){
					rf.beLeader()
					send(rf.voteCh) //通知select goroutine
				}
			}
		}(i)
	}

}

func send(ch chan bool){
	select{
	case<-ch:
	default:
	}
	ch<-true
}

func (rf *Raft)getLastLogIndex()int{
	return len(rf.log)-1
}

func (rf *Raft)getLastLogTerm()int{
	index:=rf.getLastLogIndex()
	if index<0{
		return -1
	}
	return rf.log[index].Term
}
//--------------partB-日志append--------------


type AppendEntriesArgs struct{
	Term int
	LeaderId int
	PrevLogIndex int //最新日志之前的的索引值  日志要append的起始wei'zh
	PreLogTerm int
	Entries []Log
	LeaderCommit int

}

type AppendEntriesReply struct{
	Term int
	Success bool

}

func (rf *Raft)sendAppendEntries(server int,args *AppendEntriesArgs, reply *AppendEntriesReply)bool{
	ok:=rf.peers[server].Call("Raft.AppendEntries",args,reply)
	return ok
}

//RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs,reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term>rf.currentTerm{
		rf.beFollower(args.Term)
		send(rf.appendLogCh)
	}
	prevLogIndexTerm:=-1
	if args.PrevLogIndex>=0{
		prevLogIndexTerm=rf.log[args.PrevLogIndex].Term
	}
	if args.Term<rf.currentTerm{
		return
	}
	if prevLogIndexTerm!=args.PreLogTerm{
		return
	}
	reply.Term=rf.currentTerm
	reply.Success=true
	//
	send(rf.appendLogCh)
}

//from leader action
func (rf *Raft) startAppendLog(){
	for i:=0;i<len(rf.peers);i++{
		if i==rf.me{
			continue
		}
		go func(nodeIndex int){
			if rf.state!=Leader{
				return
			}
			args:=AppendEntriesArgs{
				Term:rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: rf.getPrevLogIndex(nodeIndex),
				PreLogTerm: rf.getPrevLogTerm(nodeIndex),
				Entries: make([]Log,0),
				LeaderCommit: rf.commitIndex,
			}
			reply:=&AppendEntriesReply{}
			response:=rf.sendAppendEntries(nodeIndex,&args,reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			//TODO 这个锁很重要
			if response{
				return 
			}
			if reply.Term>rf.currentTerm{
				rf.beFollower(reply.Term)
				return
			}
		}(i)
	}

}

func (rf *Raft)getPrevLogIndex(i int)int{
	//DPrintf("getPrevLogIndex %d,%d",i,len(rf.nextIndex))
	return rf.nextIndex[i]-1
}

func (rf *Raft)getPrevLogTerm(i int)int{
	prevLogIndex:=rf.getPrevLogIndex(i)
	if prevLogIndex<0{return -1}
	return rf.log[prevLogIndex].Term
}










//---------partC-持久化部分--------------------
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}



