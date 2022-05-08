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
	"bytes"
	"sync"
	"sync/atomic"

	"math/rand"
	"sort"
	"time"
	//"fmt"

	"6.824/labgob"
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

//
// A Go object implementing a single Raft peer.
//

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

const HeartbeatsInterval = time.Millisecond * 100

type Entry struct {
	Command interface{}
	Term    int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	LeaderId    int  // so follower can redirect clients
	status      int8 // follwer , candidate , leader
	term        int  // current term
	commitIndex int  // index of highest log entry known to be commited
	lastApplied int  // index of highest log entry applied to state machine
	logs        []Entry

	// reinitialized after election
	nextIndex  []int // for each server ,index of the next log entry to send to that server
	matchIndex []int // for each server ,index of highest log entry known to be replicated on server

	electionTimer  *time.Timer // election timeout
	heartbeatTimer *time.Timer // 心跳计时器
	votedFor       int         // in this term ,had voted

	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
	applyCh           chan ApplyMsg
	notifyApplyLogCh  chan struct{}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.term
	isleader = (rf.status == LEADER)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.logs)
	e.Encode(rf.votedFor)
	state := w.Bytes()

	ws := new(bytes.Buffer)
	es := labgob.NewEncoder(w)
	es.Encode(rf.lastIncludedIndex)
	es.Encode(rf.lastIncludedTerm)
	es.Encode(rf.snapshot)
	snapshot := ws.Bytes()

	//rf.persister.SaveRaftState(state ,snapshot)
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.term = 0
		rf.logs = make([]Entry, 1)
		rf.logs[0] = Entry{nil, 0}
		rf.votedFor = -1
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&rf.term)
	d.Decode(&rf.logs)
	d.Decode(&rf.votedFor)

	sr := bytes.NewBuffer(snapshot)
	sd := labgob.NewDecoder(sr)
	sd.Decode(&rf.lastIncludedIndex)
	sd.Decode(&rf.lastIncludedTerm)
	sd.Decode(&rf.snapshot)
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	ok := lastIncludedIndex >= rf.lastApplied &&
		lastIncludedIndex >= rf.lastIncludedIndex &&
		snapshot != nil && len(snapshot) > 0

	if ok {
		rf.lastApplied = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex
		rf.notifyApplyLogCh <- struct{}{}
	}
	DPrintf("Cand %d：lastIncludedIndex %d  lastApplied %d and ok is %v", rf.me, lastIncludedIndex, rf.lastApplied, ok)
	return ok
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

func (rf *Raft) calcIndex(index int) int {
	return index - rf.lastIncludedIndex
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d ：a new snapshot lastApplied %d and index %d", rf.me, rf.lastApplied, index)

	old := rf.lastIncludedIndex

	rf.lastIncludedTerm = rf.logs[rf.calcIndex(index)].Term
	rf.lastIncludedIndex = index
	rf.snapshot = make([]byte, len(snapshot))
	copy(rf.snapshot, snapshot)

	rf.logs = rf.logs[index-old:]
	rf.persist()

}

type RequestInstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type RequestInstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *RequestInstallSnapshotArgs, reply *RequestInstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	if args.Term < rf.term || args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	} else if args.Term > rf.term {
		rf.term = args.Term
		rf.status = FOLLOWER
		rf.votedFor = -1
		rf.persist()
	}

	rf.resetTimeout()

	// snapshot is out of date
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	if args.LastIncludedIndex < rf.getIndex()-1 {
		DPrintf("%d ：argsLast %d, rfLast %d,log len %d ,rf.Index %d",
			rf.me, args.LastIncludedIndex, rf.lastIncludedIndex, len(rf.logs), rf.getIndex()-1)
		// for i := 0 ; i < len(rf.logs) ; i ++ {
		// 	if rf.lastIncludedIndex + i == args.LastIncludedIndex {
		// 		rf.logs = rf.logs[i:]
		// 		break
		// 	}
		// }
		rf.logs = rf.logs[args.LastIncludedIndex-rf.lastIncludedIndex:]

		DPrintf("%d ：argsLast %d, rfLast %d,log len %d ,rf.Index %d",
			rf.me, args.LastIncludedIndex, rf.lastIncludedIndex, len(rf.logs), args.LastIncludedIndex+len(rf.logs)-1)
	} else {
		rf.logs = make([]Entry, 1)
		rf.logs[0] = Entry{nil, args.LastIncludedTerm}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.persist()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm , for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type RequestAppendEntriesArgs struct {
	Term         int     // leader's term
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // log entries to store
	LeaderCommit int     // leader's commitIndex
}

type RequestAppendEntriesReply struct {
	Term    int  // currentTerm for leader to update itself
	Success bool // true if follower contained entry matching
	// prevLogIndex and prevLogTerm
	ConflictIndex int
	ConflictTerm  int
}

//
// example RequestVote RPC handler.
//

// luanch an election
func (rf *Raft) election(term int) int {
	rf.mu.Lock()
	args := RequestVoteArgs{
		term,
		rf.me,
		rf.getIndex() - 1,
		rf.logs[len(rf.logs)-1].Term}
	rf.votedFor = rf.me // vote itself
	rf.mu.Unlock()
	rf.persist()

	voteCh := make(chan int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		go func(u int) {
			if u == rf.me {
				voteCh <- 1
				return
			}
			reply := RequestVoteReply{}
			if rf.status == CANDIDATE && rf.sendRequestVote(u, &args, &reply) {
				//DPrintf("========== %v ======== me is %d term is %d receive from %d", reply, rf.me, term, u)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.term {
					rf.term = reply.Term
					rf.votedFor = -1
					rf.persist()
					rf.status = FOLLOWER
					voteCh <- 0
				} else if reply.VoteGranted {
					voteCh <- 1
				}
			} else {
				voteCh <- 0
			}

		}(i)
	}
	voteCnt, cnt := 0, 0
	timeout := time.After(time.Millisecond * 50)
	voteCounting:
		for {
			select {
			case v := <-voteCh:
				voteCnt += v
				cnt++
				//DPrintf("%d : %d", rf.me, voteCnt)
				if voteCnt > len(rf.peers)/2 || cnt-voteCnt > len(rf.peers)/2 {
					//return voteCnt
					break voteCounting
				}
			case <-timeout:
				break voteCounting
			}
		}
	return voteCnt
}

func (rf *Raft) launchEelection() {
	// To begin an election, a follower increments its current term
	// and transitions to candidate state
	rf.mu.Lock()
	rf.term++
	rf.status = CANDIDATE
	term := rf.term
	rf.resetTimeout()
	rf.mu.Unlock()

	majority := len(rf.peers)/2 + 1

	// luanch election
	accquire_votes := rf.election(term)

	// receive term larger than me or votes not enough
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if accquire_votes >= majority && rf.status == CANDIDATE && term == rf.term {
		rf.convertToLeader()
	} else {
		rf.status = FOLLOWER
	}
	DPrintf("%d receive %d vote and status is %d in term %d and now term is %d",
		rf.me, accquire_votes, rf.status, term, rf.term)

}

func (rf *Raft) applyLogs(applyCh chan ApplyMsg) {
	for !rf.killed() {
		<-rf.notifyApplyLogCh
		//time.Sleep(time.Millisecond * 10)

		var appliedMsgs = make([]ApplyMsg, 0)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				if rf.lastApplied <= rf.lastIncludedIndex {
					continue
				}
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.calcIndex(rf.lastApplied)].Command,
					CommandIndex: rf.lastApplied}
				appliedMsgs = append(appliedMsgs, applyMsg)
			}
		}()

		for _, msg := range appliedMsgs {
			if msg.CommandIndex > rf.lastIncludedIndex {
				applyCh <- msg
			}
		}

	}
}

func (rf *Raft) getAppendEntriesArgs(peerId int) (*RequestAppendEntriesArgs, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := RequestAppendEntriesArgs{}
	args.Term = rf.term
	args.LeaderId = rf.me
	if rf.status != LEADER || rf.nextIndex[peerId] <= rf.lastIncludedIndex {
		return &args, false
	}

	args.PrevLogIndex = rf.nextIndex[peerId] - 1
	if args.PrevLogIndex >= rf.lastIncludedIndex {
		args.PrevLogTerm = rf.logs[rf.calcIndex(args.PrevLogIndex)].Term
	}

	args.Entries = make([]Entry, 0)
	nextIndex := rf.nextIndex[peerId]
	args.Entries = append(args.Entries, rf.logs[rf.calcIndex(nextIndex):]...)

	args.LeaderCommit = rf.commitIndex

	return &args, true
}

func (rf *Raft) updateCommitIndex() {
	sortMatchIndex := make([]int, 0)
	rf.matchIndex[rf.me] = rf.getIndex() - 1
	sortMatchIndex = append(sortMatchIndex, rf.matchIndex...)
	sort.Ints(sortMatchIndex)

	new_commitIndex := sortMatchIndex[len(rf.peers)/2]
	if new_commitIndex > rf.commitIndex && rf.logs[rf.calcIndex(new_commitIndex)].Term == rf.term {
		rf.commitIndex = new_commitIndex
		rf.notifyApplyLogCh <- struct{}{}
		//DPrintf("%d:notifyApplyLog send", rf.me)
	}
}

func (rf *Raft) updateNextIndex(u int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {

	DPrintf("%d to %d ：ConflictIndex:%d ：ConflictTerm %d, includeIndex：%d ,prevLogIndex %d",
		rf.me, u, reply.ConflictIndex, reply.ConflictTerm, rf.lastIncludedIndex, args.PrevLogIndex)
	// PrevLogIndex >= len(u.logs)
	// reply.ConflictTerm = len(u.logs) - 1
	if reply.ConflictTerm == -1 {
		rf.nextIndex[u] = reply.ConflictIndex + 1
		return
	}

	// find the last term == conflictTerm
	for i := args.PrevLogIndex; i > rf.lastIncludedIndex; i-- {
		DPrintf("%d：i： calc %d ,term %d", i, rf.calcIndex(i), rf.logs[rf.calcIndex(i)].Term)
		if rf.logs[rf.calcIndex(i)].Term == reply.ConflictTerm {
			rf.nextIndex[u] = i + 1
			return
		}
	}

	// ex : leader 1 2 4  follower 1 3
	rf.nextIndex[u] = reply.ConflictIndex

}

// fail sendInstallSnapshot or term < reply.Term return false
func (rf *Raft) sendSnapshot(peerId int) bool {
	ok := true
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.nextIndex[peerId] > rf.lastIncludedIndex {
		return true
	}

	var success = make(chan bool, 1)
	args := RequestInstallSnapshotArgs{}
	args.Term = rf.term
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.lastIncludedIndex
	args.LastIncludedTerm = rf.lastIncludedTerm
	args.Data = rf.snapshot

	reply := RequestInstallSnapshotReply{}
	go func() {
		DPrintf("%d to %d：sendInstallSnapshot", rf.me, peerId)
		if rf.sendInstallSnapshot(peerId, &args, &reply) {
			if rf.term != args.Term || rf.status != LEADER {
				success <- false
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.term {
				rf.term = reply.Term
				rf.votedFor = -1
				rf.status = FOLLOWER
				rf.persist()
			} else if args.LastIncludedIndex > rf.nextIndex[peerId] {
				rf.matchIndex[peerId] = args.LastIncludedIndex
				rf.nextIndex[peerId] = args.LastIncludedIndex + 1
			}
			success <- true
		} else {
			success <- false
		}
	}()

	select {
	case ok = <-success:
	case <-time.After(time.Millisecond * 10):
		ok = false
	}

	return ok
}

func (rf *Raft) heartbeats() {
	term := rf.term
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(u int) {
				if !rf.sendSnapshot(u) {
					return
				}
				args, isSend := rf.getAppendEntriesArgs(u)
				reply := RequestAppendEntriesReply{}
				if term != args.Term || !isSend {
					return
				}

				ok := rf.sendAppendEntries(u, args, &reply)

				if !ok || args.Term != rf.term {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.term {
					rf.term = reply.Term
					rf.votedFor = -1
					rf.status = FOLLOWER
					rf.persist()
					return
				}
				if rf.status == LEADER {
					if reply.Success {
						rf.matchIndex[u] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[u] = rf.matchIndex[u] + 1
						// update commitIndex
						if len(args.Entries) != 0 {
							rf.updateCommitIndex()
						}

					} else {
						rf.updateNextIndex(u, args, &reply)
					}
				}
			}(i)
		}
	}

}

func (rf *Raft) convertToLeader() {
	rf.status = LEADER
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getIndex()
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	reply.VoteGranted = false

	if args.Term < rf.term {
		// DPrintf("candidate %d term is %d and my term is %d and votedFor is %d",
		// 	args.CandidateId, args.Term, rf.term, rf.votedFor)
		return
	} else if args.Term > rf.term {
		rf.term = args.Term
		rf.status = FOLLOWER
		rf.votedFor = -1
		rf.persist()
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	// candidate term don't large than me ,
	// or in this term had voted
	//lastIndex := len(rf.logs) - 1
	lastIndex := rf.getIndex() - 1
	if args.LastLogTerm < rf.logs[rf.calcIndex(lastIndex)].Term ||
		(args.LastLogTerm == rf.logs[rf.calcIndex(lastIndex)].Term && args.LastLogIndex < lastIndex) {
		// DPrintf("%d not vote to %d in term %d and my lastLogTerm is %d len is %d args lastterm is %d  len is %d",
		// 	rf.me, args.CandidateId, args.Term, rf.logs[len(rf.logs)-1].Term, len(rf.logs)-1, args.LastLogTerm, args.LastLogIndex)
		return
	}

	rf.votedFor = args.CandidateId
	rf.resetTimeout()
	reply.VoteGranted = true
	// DPrintf("%d vote to %d in term %d reply is %v  args.LastLogTerm %d,myLastLogTerm %d",
	//  rf.me, args.CandidateId, args.Term, reply, args.LastLogTerm,rf.logs[rf.calcIndex(lastIndex)].Term)
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// 1),reply false if args.term < rf.term
	if args.Term < rf.term {
		//DPrintf("leader term is %d and my term is %d", args.Term, rf.term)
		return
	} else if args.Term > rf.term {
		rf.term = args.Term
		rf.votedFor = -1
		rf.LeaderId = args.LeaderId
		rf.status = FOLLOWER
		rf.persist()
	}

	rf.resetTimeout()

	if args.PrevLogIndex < rf.lastIncludedIndex {
		return
	}

	// 2) reply false if log don't contains an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex >= rf.getIndex() {
		//reply.ConflictIndex = len(rf.logs) - 1
		reply.ConflictIndex = rf.getIndex() - 1
		return
	}

	// find the first index of conflict term
	if rf.logs[rf.calcIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.logs[rf.calcIndex(args.PrevLogIndex)].Term

		DPrintf("%d：log term %d , args.PrevLogTerm %d", rf.me, rf.logs[rf.calcIndex(args.PrevLogIndex)].Term, args.PrevLogTerm)

		for idx := rf.lastIncludedIndex + 1; idx <= args.PrevLogIndex; idx++ {
			DPrintf("%d：log term %d , conflictTerm %d", rf.me, rf.logs[rf.calcIndex(idx)].Term, reply.ConflictTerm)
			if rf.logs[rf.calcIndex(idx)].Term == reply.ConflictTerm {
				reply.ConflictIndex = idx
				break
			}
		}
		return
	}
	// 3) if an existing entry conflicts whith a new one(same index but different term)
	// delete the existing entry and all that follow it
	// 4) append new entries
	for i := 0; i < len(args.Entries); i++ {
		index := i + args.PrevLogIndex + 1
		if index >= rf.getIndex() {
			rf.logs = append(rf.logs, args.Entries[i])
		} else {
			FL_term := rf.logs[rf.calcIndex(index)].Term
			LD_term := args.Entries[i].Term
			if FL_term != LD_term {
				//DPrintf("me %d is different leader ,logs %v" ,rf.me ,rf.logs)
				rf.logs = rf.logs[0:rf.calcIndex(index)]
				rf.logs = append(rf.logs, args.Entries[i:]...)
				//DPrintf("me %d is different leader ,logs %v" ,rf.me ,rf.logs)
				break
			}
		}
	}

	// 5) if leaderCommit > commitedIndex
	if args.LeaderCommit > rf.commitIndex {
		//old := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, rf.getIndex()-1)
		rf.notifyApplyLogCh <- struct{}{}
		//DPrintf("%d - %d - %d adn leader term %d：%d update CommitIndex %d",rf.me,rf.status,reply.Term,args.Term,old,rf.commitIndex )
	}
	// DPrintf("%d：commitIndex:%v %v ,lastInclude：%d ,lastApplied： %d",
	//  rf.me , rf.commitIndex , args.LeaderCommit,rf.lastIncludedIndex ,rf.lastApplied)
	reply.Success = true

	if len(args.Entries) > 0 {
		rf.persist()
		//DPrintf("%d: notifyApplyLog send ,len ch %d", rf.me,len(rf.notifyApplyLogCh))
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
func (rf *Raft) sendInstallSnapshot(server int, args *RequestInstallSnapshotArgs, reply *RequestInstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
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

func (rf *Raft) getIndex() int {
	return rf.lastIncludedIndex + len(rf.logs)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.term
	isLeader = rf.status == LEADER
	if isLeader {
		//index = len(rf.logs)
		index = rf.getIndex()
		entry := Entry{command, term}
		rf.logs = append(rf.logs, entry)
		rf.persist()

		DPrintf("===leader is me %d term %d == , now index is %d  lastIncludedIndex is %d, len is %d",
			rf.me, rf.term, rf.getIndex(), rf.lastIncludedIndex, len(rf.logs))
		rf.resetHeartbeatTime(time.Duration(0))
	}

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

func randElectionTimeout() time.Duration {
	var min, max int64 = 250, 400
	return time.Duration(rand.Int63n(max-min)+min) * time.Millisecond
}

func (rf *Raft) resetTimeout() {
	rf.electionTimer.Reset(randElectionTimeout())
}

func (rf *Raft) resetHeartbeatTime(interval time.Duration)  {
	rf.heartbeatTimer.Reset(interval)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		switch rf.status {
		case FOLLOWER:
			<-rf.electionTimer.C
			rf.launchEelection()
		case LEADER:
			<-rf.heartbeatTimer.C
			rf.resetHeartbeatTime(HeartbeatsInterval)
			go rf.heartbeats()
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
	rf.applyCh = applyCh
	rf.notifyApplyLogCh = make(chan struct{}, 10)

	// Your initialization code here (2A, 2B, 2C).
	rf.electionTimer = time.NewTimer(randElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(HeartbeatsInterval)

	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLogs(applyCh)

	return rf
}
