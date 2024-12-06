package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"
)

type Member struct {
	ID          int
	Alive       bool
	IsLeader    bool
	IsCandidate bool
	Votes       int
	FailedList  []int
}

type Quorum struct {
	Members []*Member
	mu      sync.Mutex
	Leader  *Member
	Current int
	Fail    bool
}

func main() {

	if len(os.Args) != 2 {
		fmt.Println("Usage: ./main {quorum_size}")
		return
	}

	size, err := strconv.Atoi(os.Args[1])
	if err != nil || size < 1 {
		fmt.Println("Invalid quorum size. Please provide a positive integer.")
		return
	}

	fmt.Printf("Starting quorum with %d members\n", size)

	q := NewQuorum(size)
	q.Start()

}

func NewQuorum(size int) *Quorum {
	if size < 1 {
		panic("Quorum size must be greater than 0")
	}

	q := &Quorum{
		Members: make([]*Member, size),
		Current: size,
	}

	for i := 0; i < size; i++ {
		q.Members[i] = &Member{ID: i, Alive: true, FailedList: make([]int, 0, size-1)}
	}

	return q
}

func (q *Quorum) Start() {
	ctx, cancel := context.WithCancel(context.Background())

	for _, member := range q.Members {
		go q.StartHeartbeat(ctx, member)
	}

	go q.ElectLeader()
	go q.ListenCommand(ctx, cancel)

	<-ctx.Done()
	fmt.Println("Quorum has been shut down.")
}

func (q *Quorum) StartHeartbeat(ctx context.Context, member *Member) {
	fmt.Printf("Member %d: Hi\n", member.ID)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// alive
			time.Sleep(time.Second)

			if member.Alive && !q.Fail {
				q.mu.Lock()
				for _, peer := range q.Members {
					quorums := len(q.Members)
					if peer.ID != member.ID && !slices.Contains(member.FailedList, peer.ID) {
						quorums--
						if !peer.Alive {
							if quorums > 1 {
								fmt.Printf("Member %d: failed heartbeat with Member %d\n", member.ID, peer.ID)
								member.FailedList = append(member.FailedList, peer.ID)
							} else {
								fmt.Printf("Member %d: no response from other users\n", member.ID)
								q.Kill(peer.ID, true)
							}
						}
					}
				}
				q.mu.Unlock()
			}
		}
	}
}

func (q *Quorum) ElectLeader() {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, member := range q.Members {
		member.Votes = 0
		member.IsCandidate = false
	}

	var candidates []int
	for idx, member := range q.Members {
		if member.Alive && rand.Intn(2) == 1 {
			member.IsCandidate = true
			candidates = append(candidates, idx)
			fmt.Printf("Member %d: I want to be leader\n", member.ID)
		}
	}

	if len(candidates) == 0 {
		// 當剛好都沒人要選 指派0來參選
		candidates = append(candidates, 0)
		fmt.Printf("Member %d: I want to be leader\n", q.Members[0].ID)
	}

	for _, member := range q.Members {
		if !member.Alive {
			continue
		}

		if member.IsCandidate {
			member.Votes = 1
			continue
		}

		target := rand.Intn(len(candidates))
		q.Members[candidates[target]].Votes++
		fmt.Printf("Member %d: Accept member %d to be leader\n", member.ID, q.Members[target].ID)

	}

	newLeaderIdx, maxVotes := q.FindLeader()
	q.Leader = q.Members[newLeaderIdx]
	q.Leader.IsLeader = true
	fmt.Printf("Member %d voted to be leader: (%d > %d/2)\n", q.Leader.ID, maxVotes, len(q.Members))

}

func (q *Quorum) FindLeader() (int, int) {
	var leaderPossible []int
	maxVotes := math.MinInt

	for idx, member := range q.Members {
		if member.IsCandidate && member.Votes > maxVotes {
			leaderPossible = make([]int, 0)
			leaderPossible = append(leaderPossible, idx)
			maxVotes = member.Votes
			continue
		}

		if member.IsCandidate && member.Votes == maxVotes {
			leaderPossible = append(leaderPossible, idx)
			continue
		}
	}

	if len(leaderPossible) == 1 {
		return leaderPossible[0], maxVotes
	}

	return leaderPossible[rand.Intn(len(leaderPossible))], maxVotes

}

func (q *Quorum) ListenCommand(ctx context.Context, cancel context.CancelFunc) {
	for {
		var cmd string
		var memberID int
		fmt.Scan(&cmd, &memberID)

		if cmd == "kill" {
			if memberID < 0 || memberID > len(q.Members) {
				fmt.Println("invalid memberID")
				continue
			}

			q.Kill(memberID, false)
			q.CheckQuorum(cancel)
		}
	}
}

func (q *Quorum) Kill(memberID int, isLeader bool) {
	q.mu.Lock()
	q.Members[memberID].Alive = false
	q.Current--
	q.mu.Unlock()

	// time.Sleep(3 * time.Second)
	fmt.Printf("Member %d: kick out of quorum: ", memberID)
	if isLeader || q.Current == 1 {
		fmt.Println("leader decision")
	} else {
		fmt.Printf("(%d > current/2)\n", q.Current)
	}

	if q.Leader != nil && q.Leader.ID == memberID {
		q.ElectLeader()
	}
}

func (q *Quorum) CheckQuorum(cancel context.CancelFunc) {
	alives := 0
	for _, member := range q.Members {
		if member.Alive {
			alives++
		}
	}

	if alives <= len(q.Members)/2 {
		q.mu.Lock()
		q.Fail = true
		q.mu.Unlock()
		fmt.Printf("Quorum failed: (%d > total/2)\n", alives)
		cancel()
	}
}
