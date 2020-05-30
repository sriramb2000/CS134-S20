package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

import "sort"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	currSeqNum int
}


type Op struct {
	// Your data here.
	OpType    string
	Id		  int64
	Gid		  int64
	Servers   []string
	ShardNum  int
	ConfigNum int	
}


// Sorted List to help Rebalance Load 
type GroupCount struct {
	gid int64
	numShards int
}

type GroupCountList []GroupCount

func (l GroupCountList) Init() { sort.Sort(l) }
func (l GroupCountList) Len() int { return len(l) }
func (l GroupCountList) Less(i, j int) bool { return l[i].numShards < l[j].numShards }
func (l GroupCountList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l GroupCountList) Insert(gc GroupCount) GroupCountList {
	i := sort.Search(len(l), func(j int) bool { return l[j].numShards >= gc.numShards })
	if (i < len(l) && l[i].gid == gc.gid) {
		return l
	} else {
		l = append(l, GroupCount{})
		if (i < len(l)) {
		 copy(l[i+1:], l[i:])
		}
		l[i] = gc
		return l
	}
}
func (l GroupCountList) PopMin() (GroupCount, GroupCountList) {
	min := l[0]
	l = l[1:]
	return min, l
}
func (l GroupCountList) GetMin() { return l[0] }
func (l GroupCountList) PopMax() (GroupCount, GroupCountList) {
	max := l[len(l) - 1]
	l = l[:len(l)-1]
	return max, l
}
func (l GroupCountList) GetMax() { return l[len(l) - 1] }

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

// Initialize a copy of the latest conifiguration
func (sm* ShardMaster) NextConfig() *Config {
	lastConfig := &sm.configs[len(sm.configs) - 1];

	nextConfig := Config{}
	nextConfig.Num = lastConfig.Num + 1
	for shardNum, gid := range(lastConfig.Shards) {
		nextConfig.Shards[shardNum] = gid
	}
	nextConfig.Groups = make(map[int64][]string)
	for gid, servers := range(lastConfig.Groups) {
		nextConfig.Groups[gid] = servers
	}
	
	return &nextConfig
}

func (sm* ShardMaster) BuildSorted(config *Config) {
	min := make(MinHeap, 0)
	max := make(MaxHeap, 0)
	heap.Init(&min)
	heap.Init(&max)

	shardCount := make(map[int64]int)
	// initialize empty count map
	for gid, _ := range(config.Groups) {
		shardCount[gid] = 0
	}
	// populate count map
	for _, gid := range(config.Shards) {
		_, ok = config.Groups[group] // make sure that the gid is valid
		if (ok) {
			shardCount[gid]++
		}
	}
	// populate heap
	for gid, count := range(shardCount) {
		tmp := GroupCount{gid, count}
		heap.Push(&min, &tmp)
		heap.Push(&max, &tmp)
	}
}

// Runs Paxos until some Consensus Op is reached for the given sequence numebr
func (sm* ShardMaster) Consensify(seq int, val Op) Op {
	sm.px.Start(seq, val)

	to := 10 * time.Millisecond
	for {
		status, value := sm.px.Status(seq)
		if status == paxos.Decided {
			return value.(Op) // cast to Op
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

func (sm* KVPaxos) PassOp(proposalOp Op) error {
	for {
		seqNum := sm.currSeqNum
		sm.currSeqNum++

		status, res := sm.px.Status(seqNum)

		var acceptedOp Op
		if status == paxos.Decided { // If this instance has already been decided, proceed with decided value
			acceptedOp = res.(Op)
		} else { // Otherwise propose our own Op
			acceptedOp = sm.Consensify(seqNum, proposalOp)
		}

		sm.CommitOp(acceptedOp)
		sm.px.Done(seqNum) // This Paxos Peer can safely forget about this instance now that the value has been committed

		if proposalOp.Id == acceptedOp.Id { // We can respond to client
			break
		}
	}
	return nil
}

func (sm *ShardMaster) CommitOp(operation Op) {

}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.

	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
