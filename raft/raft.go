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
	"assignment2/labrpc"
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Raft's PersistData
type LogData struct {
	Command interface{}
	Term    int
}

type PersistData struct {
	CurrentTerm int
	VoteFor     int
	Log         []LogData
}

// 3 Roles in Raft
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower "
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader   "
	default:
		return "Unknown  "
	}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	applyCh   chan ApplyMsg

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	voteFor     int
	log         []LogData

	role        Role
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	chTimerDone chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := (rf.role == Leader)

	log.Printf("(%d,%s,%d)   @GetState   RETURN (%d,%t)\n", rf.me, rf.role, rf.currentTerm, term, isleader)

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.

// call in lock
func (rf *Raft) savePersist() {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	pData := PersistData{rf.currentTerm, rf.voteFor, rf.log}

	if err := encoder.Encode(pData); err != nil {
		log.Printf("(%d,%s,%d)   @savePersist   encoding error: %+v\n", rf.me, rf.role, rf.currentTerm, err)
		return
	}

	// log.Printf("(%d,%s,%d)   @savePersist   SAVE\n", rf.me, rf.role, rf.currentTerm)
	rf.persister.SaveRaftState(buffer.Bytes())
}

// restore previously persisted state.
// call in lock
func (rf *Raft) readPersist() {
	data := rf.persister.ReadRaftState()
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	var pData PersistData

	if err := decoder.Decode(&pData); err != nil {
		log.Printf("(%d,%s,%d)   @readPersist   decoding error: %+v\n", rf.me, rf.role, rf.currentTerm, err)
		rf.currentTerm = 0
		rf.voteFor = -1
		rf.log = []LogData{{nil, 0}}
		return
	}

	rf.currentTerm = pData.CurrentTerm
	rf.voteFor = pData.VoteFor
	rf.log = pData.Log
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	}
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && (args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)) {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

	rf.savePersist()

	log.Printf("(%d,%s,%d)   @RequestVote     args = %+v reply = %+v\n", rf.me, rf.role, rf.currentTerm, args, *reply)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogData
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.resetFollowerTimer()

	if rf.role == Candidate {
		rf.role = Follower
	}
	if rf.role == Candidate || rf.voteFor == -1 {
		rf.voteFor = args.LeaderId
	}
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	}

	reply.Term = rf.currentTerm

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
	} else {
		if !(args.PrevLogIndex+1 == len(rf.log) && len(args.Entries) == 0) {
			log.Printf("(%d,%s,%d)   @AppendEntries   BEFORE LOG = %+v\n", rf.me, rf.role, rf.currentTerm, rf.log)
			rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
			log.Printf("(%d,%s,%d)   @AppendEntries   AFTER  LOG = %+v\n", rf.me, rf.role, rf.currentTerm, rf.log)
		}
		reply.Success = true

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)

			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				rf.applyCh <- ApplyMsg{rf.lastApplied, rf.log[rf.lastApplied].Command, false, []byte{}}
				log.Printf("(%d,%s,%d)   @AppendEntries   APPLIED %d\n", rf.me, rf.role, rf.currentTerm, rf.lastApplied)
			}
		}
	}

	rf.savePersist()

	log.Printf("(%d,%s,%d)   @AppendEntries   args = %+v reply = %+v\n", rf.me, rf.role, rf.currentTerm, args, *reply)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("(%d,%s,%d)   @Start           command = %+v\n", rf.me, rf.role, rf.currentTerm, command)

	if rf.role != Leader {
		return -1, -1, false
	}

	log.Printf("(%d,%s,%d)   @Start           BEFORE LOG = %+v\n", rf.me, rf.role, rf.currentTerm, rf.log)
	rf.log = append(rf.log, LogData{command, rf.currentTerm})
	log.Printf("(%d,%s,%d)   @Start           AFTER  LOG = %+v\n", rf.me, rf.role, rf.currentTerm, rf.log)

	rf.savePersist()

	index := len(rf.log) - 1
	term := rf.currentTerm
	isLeader := true
	return index, term, isLeader
}

// call in lock
func (rf *Raft) updateTerm(newTerm int) {
	rf.currentTerm = newTerm
	rf.voteFor = -1

	if rf.role != Follower {
		// Candidate / Leader -> Follower
		log.Printf("(%d,%s,%d)   ROLE SWITCH TO %s @updateTerm\n", rf.me, rf.role, rf.currentTerm, Follower)
		rf.role = Follower

		rf.resetFollowerTimer()
	}
}

var goroutineIDCounter int64

// call in lock
func (rf *Raft) resetFollowerTimer() {
	if rf.chTimerDone != nil {
		close(rf.chTimerDone)
		rf.chTimerDone = nil
	}
	rf.chTimerDone = make(chan bool)

	go func(done chan bool) {
		// random interval between 150-300ms
		interval := time.Millisecond * time.Duration(150+rand.Intn(150))
		id := atomic.AddInt64(&goroutineIDCounter, 1)
		// rf.mu.Lock()
		// log.Printf("(%d,%s,%d)   @FollowerTimer   Timer %d Setup\n", rf.me, rf.role, rf.currentTerm, id)
		// rf.mu.Unlock()

		select {
		case <-time.After(interval):
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role == Leader {
				return
			}
			log.Printf("(%d,%s,%d)   @FollowerTimer   Timer %d Timeout, become candidate\n", rf.me, rf.role, rf.currentTerm, id)

			// Follower -> Candidate
			log.Printf("(%d,%s,%d)   ROLE SWITCH TO %s @election\n", rf.me, rf.role, rf.currentTerm, Candidate)
			rf.role = Candidate
			rf.voteFor = rf.me
			rf.currentTerm += 1
			args := RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term}
			voteCnt := 1

			rf.savePersist()
			rf.resetFollowerTimer()

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(peerId int) {
					// rf.mu.Lock()
					log.Printf("(%d,%s,%d)   @FollowerTimer   >>> sendRequestVote to %d\n", rf.me, rf.role, rf.currentTerm, peerId)
					// rf.mu.Unlock()

					var reply RequestVoteReply
					ok := rf.sendRequestVote(peerId, args, &reply)

					rf.mu.Lock()
					defer rf.mu.Unlock()

					if !ok {
						log.Printf("(%d,%s,%d)   @FollowerTimer   sendRequestVote Error\n", rf.me, rf.role, rf.currentTerm)
						return
					}
					log.Printf("(%d,%s,%d)   @FollowerTimer   <<< sendRequestVote to %d get %+v\n", rf.me, rf.role, rf.currentTerm, peerId, reply)

					if reply.VoteGranted {
						voteCnt += 1
					}

					if voteCnt >= len(rf.peers)/2+1 && rf.role == Candidate {
						// Candidate -> Leader
						log.Printf("(%d,%s,%d)   ROLE SWITCH TO %s @electionWin\n", rf.me, rf.role, rf.currentTerm, Leader)
						rf.role = Leader
						if rf.chTimerDone != nil {
							close(rf.chTimerDone)
							rf.chTimerDone = nil
						}

						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = 0
						}

						rf.chTimerDone = make(chan bool)
						go rf.Heartbeat(rf.chTimerDone)
					}

				}(i)
			}
		case <-done:
			// rf.mu.Lock()
			// log.Printf("(%d,%s,%d)   @FollowerTimer   Timer %d Received cancel signal", rf.me, rf.role, rf.currentTerm, id)
			// rf.mu.Unlock()
			return
		}
	}(rf.chTimerDone)
}

func (rf *Raft) Heartbeat(done chan bool) {
	for {
		rf.mu.Lock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(peerId int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.nextIndex[peerId] - 1, rf.log[rf.nextIndex[peerId]-1].Term, rf.log[rf.nextIndex[peerId]:], rf.commitIndex}
				lastLogIndex := len(rf.log) - 1
				log.Printf("(%d,%s,%d)   @Heartbeat       >>> sendAppendEntries to %d\n", rf.me, rf.role, rf.currentTerm, peerId)
				rf.mu.Unlock()

				var reply AppendEntriesReply
				ok := rf.sendAppendEntries(peerId, args, &reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if !ok {
					log.Printf("(%d,%s,%d)   @Heartbeat       sendAppendEntries Error\n", rf.me, rf.role, rf.currentTerm)
					return
				}
				log.Printf("(%d,%s,%d)   @Heartbeat       <<< sendAppendEntries to %d get %+v\n", rf.me, rf.role, rf.currentTerm, peerId, reply)

				if !reply.Success {
					// rf.nextIndex[peerId] = max(1, rf.nextIndex[peerId] - 1)
					rf.nextIndex[peerId] = max(1, len(rf.log)-int(float64(len(rf.log)-rf.nextIndex[peerId]+1)*1.1))
				} else {
					rf.nextIndex[peerId] = max(rf.nextIndex[peerId], lastLogIndex)
					rf.matchIndex[peerId] = max(rf.matchIndex[peerId], lastLogIndex)
					rf.matchIndex[rf.me] = max(rf.matchIndex[rf.me], lastLogIndex)

					matchIndexCopy := make([]int, len(rf.matchIndex))
					copy(matchIndexCopy, rf.matchIndex)
					sort.Ints(matchIndexCopy)
					majorityIndex := matchIndexCopy[(len(rf.matchIndex)-1)/2]

					log.Printf("(%d,%s,%d)   @Heartbeat       get OK  match = %+v  majority = %d,%+v\n", rf.me, rf.role, rf.currentTerm, rf.matchIndex, majorityIndex, rf.log[majorityIndex].Term)

					if rf.log[majorityIndex].Term == rf.currentTerm {
						rf.commitIndex = max(rf.commitIndex, majorityIndex)

						for rf.commitIndex > rf.lastApplied {
							rf.lastApplied += 1
							rf.applyCh <- ApplyMsg{rf.lastApplied, rf.log[rf.lastApplied].Command, false, []byte{}}
							log.Printf("(%d,%s,%d)   @Heartbeat       APPLIED %d\n", rf.me, rf.role, rf.currentTerm, rf.lastApplied)
						}
					}
				}
			}(i)
		}
		rf.mu.Unlock()

		select {
		case <-time.After(50 * time.Millisecond):
			continue
		case <-done:
			log.Printf("(%d,%s,%d)   @Heartbeat   Received cancel signal", rf.me, rf.role, rf.currentTerm)
			return
		}
	}
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	close(rf.chTimerDone)
	rf.chTimerDone = nil
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.SetOutput(io.Discard)

	rf := &Raft{}

	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.chTimerDone = nil

	// Your initialization code here.

	rf.mu.Lock()
	// log.Printf("(%d,%s,%d)   @make   peers = %+v persister = %+v\n", rf.me, rf.role, rf.currentTerm, peers, persister)
	log.Printf("(%d,%s,%d)   @make\n", rf.me, rf.role, rf.currentTerm)
	rf.resetFollowerTimer()
	// initialize from state persisted before a crash
	rf.readPersist()

	rf.mu.Unlock()

	return rf
}

/*
go test -run Election > log.txt
go test -run FailNoAgree > log.txt
go test -run ConcurrentStarts > log.txt
go test -run Rejoin > log.txt
go test -run Backup > log.txt
go test -run Persist1 > log.txt
go test -run Persist2 > log.txt
go test -run Persist3 > log.txt
*/
