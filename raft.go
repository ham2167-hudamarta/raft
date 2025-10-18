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
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	currentTerm int
	votedFor    int
	log         []LogEntry
	state       int // Follower, Candidate, or Leader

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := (rf.state == 2)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = 0
		rf.persist()
	}

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	} else {
		lastLogTerm = 0
	}

	logUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		logUpToDate = true
	} else if args.LastLogTerm == lastLogTerm {
		if args.LastLogIndex >= lastLogIndex {
			logUpToDate = true
		} else {
			logUpToDate = false
		}
	} else {
		logUpToDate = false
	}

	canVote := false
	if rf.votedFor == -1 {
		canVote = true
	} else if rf.votedFor == args.CandidateId {
		canVote = true
	} else {
		canVote = false
	}

	if canVote && logUpToDate {
		rf.votedFor = args.CandidateId
		rf.state = 0
		rf.persist()
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		if rf.electionTimer != nil {
			rf.electionTimer.Stop()
		}
		rf.resetElectionTimer()
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = 0
		rf.persist()
	}

	rf.state = 0

	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}
	rf.resetElectionTimer()

	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > len(rf.log) {
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
		if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
	}

	if len(args.Entries) > 0 {
		rf.log = rf.log[:args.PrevLogIndex]
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex > len(rf.log) {
			rf.commitIndex = len(rf.log)
		}
		rf.applyCommittedEntries()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := false

	if rf.state == 2 {
		isLeader = true
	} else {
		isLeader = false
	}

	if isLeader {
		newEntry := LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.log = append(rf.log, newEntry)
		index = len(rf.log)
		rf.persist()
	} else {
		index = -1
	}

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) resetElectionTimer() {
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.electionTimer = time.AfterFunc(timeout, func() {
		rf.startElection()
	})
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == 2 {
		return
	}

	rf.state = 1
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	} else {
		lastLogTerm = 0
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votesReceived := 1
	totalServers := len(rf.peers)

	for i := 0; i < totalServers; i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if ok {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = 0
					return
				}

				if rf.state == 1 {
					if rf.currentTerm == args.Term {
						if reply.VoteGranted {
							votesReceived++
							if votesReceived > totalServers/2 {
								rf.state = 2
								for i := 0; i < totalServers; i++ {
									rf.nextIndex[i] = len(rf.log) + 1
									rf.matchIndex[i] = 0
								}
								rf.startHeartbeats()
							} else {
								// Not enough votes yet
							}
						} else {
							// Vote was not granted
						}
					} else {
						// Term has changed
					}
				} else {
					// no longer a candidate
				}
			} else {
				// RPC failed
			}
		}(i)
	}

	rf.resetElectionTimer()
}

func (rf *Raft) startHeartbeats() {
	if rf.heartbeatTimer != nil {
		rf.heartbeatTimer.Stop()
	}

	rf.heartbeatTimer = time.AfterFunc(50*time.Millisecond, func() {
		rf.sendHeartbeats()
	})
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != 2 {
		return
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		nextIdx := rf.nextIndex[i]
		entries := []LogEntry{}
		if nextIdx > 0 {
			if nextIdx <= len(rf.log) {
				entries = rf.log[nextIdx-1:]
			}
		}
		prevLogIndex := nextIdx - 1
		prevLogTerm := 0
		if prevLogIndex > 0 {
			if prevLogIndex <= len(rf.log) {
				prevLogTerm = rf.log[prevLogIndex-1].Term
			}
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		go func(server int) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if ok {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = 0

					if rf.heartbeatTimer != nil {
						rf.heartbeatTimer.Stop()
					}
					rf.resetElectionTimer()
				} else {
					if reply.Success {
						rf.nextIndex[server] = nextIdx + len(entries)
						rf.matchIndex[server] = rf.nextIndex[server] - 1

						rf.updateCommitIndex()
					} else {
						if rf.nextIndex[server] > 1 {
							rf.nextIndex[server]--
						} else {
							// NextIndex is already at minimum
						}
					}
				}
			} else {
				// RPC failed
			}
		}(i)
	}

	rf.startHeartbeats()
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) updateCommitIndex() {
	if rf.state != 2 {
		return
	}

	for n := len(rf.log); n > rf.commitIndex; n-- {
		if n > 0 && n <= len(rf.log) {
			if rf.log[n-1].Term == rf.currentTerm {
				count := 1
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						if rf.matchIndex[i] >= n {
							count++
						} else {
							// server doesn't have entry
						}
					} else {
						// This is me
					}
				}
				if count > len(rf.peers)/2 {
					rf.commitIndex = n
					break
				} else {
					// Not enough have this entry
				}
			} else {
				// from a different term
			}
		} else {
			// out of bounds
		}
	}

	rf.applyCommittedEntries()
}

func (rf *Raft) applyCommittedEntries() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		if rf.lastApplied > 0 && rf.lastApplied <= len(rf.log) {
			msg := ApplyMsg{
				Index:   rf.lastApplied,
				Command: rf.log[rf.lastApplied-1].Command,
			}
			rf.applyCh <- msg
		}
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.state = 0

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = 0
	}

	rf.readPersist(persister.ReadRaftState())
	rf.resetElectionTimer()

	return rf
}
