package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

type Fate int

const (
	Decided   Fate = iota + 1
	Pending
	Forgotten
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	InstanceSlots         map[int]*Instance  
	DoneNumbersOfPaxosPeers []int
}

type AcceptorState struct {
	PromiseNumber int
	AcceptNumber int
	AcceptValue interface{}
}

type Instance struct {
	isDecided bool
	AcceptorState *AcceptorState
}

// Prepare phase for Acceptor
type PrepareArgs struct {
	SequenceNum int
	NewProposeNum int
}

type PrepareReply struct {
	AcceptorState *AcceptorState
	Error string
}

func (px *Paxos) PrepareHandler(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	sequenceNum, newProposeNum := args.SequenceNum, args.NewProposeNum
	instance, ok := px.InstanceSlots[sequenceNum]

	if ok {
		acceptorState := instance.AcceptorState
		if newProposeNum > acceptorState.PromiseNumber {
			acceptorState.PromiseNumber = newProposeNum

			reply.Error = "OK"
			reply.AcceptorState = acceptorState
		} else {
			reply.Error = "Rejected"
		}
	} else {
		newAcceptorState := AcceptorState{PromiseNumber: newProposeNum, AcceptNumber:  -1, AcceptValue: nil}
		px.InstanceSlots[sequenceNum] = &Instance{isDecided: false, AcceptorState: &newAcceptorState}

		reply.Error = "OK"
		reply.AcceptorState = &newAcceptorState
	}
	return nil
}

// Accept phase for Acceptor
type AcceptArgs struct {
	SequenceNum    int
	NewAcceptNum   int
	NewAcceptValue interface{}
}

type AcceptReply struct {
	Error       string
}

func (px *Paxos) AcceptHandler(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	sequenceNum, newAcceptNum, newAcceptValue := args.SequenceNum, args.NewAcceptNum, args.NewAcceptValue
	instance, ok := px.InstanceSlots[sequenceNum]

	if ok {
		acceptorState := instance.AcceptorState

		if newAcceptNum >= acceptorState.PromiseNumber {
			acceptorState.PromiseNumber = newAcceptNum
			acceptorState.AcceptNumber = newAcceptNum
			acceptorState.AcceptValue = newAcceptValue

			reply.Error = "OK"
		} else {
			reply.Error = "Rejected"
		}
	} else {
		newAcceptorState := AcceptorState{PromiseNumber: newAcceptNum, AcceptNumber:  newAcceptNum, AcceptValue: newAcceptValue}
		px.InstanceSlots[sequenceNum] = &Instance{isDecided: false, AcceptorState: &newAcceptorState}
		reply.Error = "OK"
	}
	return nil
}

// Learn phase for Acceptor
type LearnArgs struct {
	SequenceNum       int
	AcceptValue interface{}
}

type LearnReply struct {
	Error string
}

func (px *Paxos) LearnHandler(args *LearnArgs, reply *LearnReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	sequenceNum, AcceptValue := args.SequenceNum, args.AcceptValue
	instance, ok := px.InstanceSlots[sequenceNum]

	if ok {
		instance.AcceptorState.AcceptValue = AcceptValue
		instance.isDecided = true
	} else {
		newAcceptorState := AcceptorState{PromiseNumber: -1, AcceptNumber:  -1, AcceptValue: AcceptValue}
		px.InstanceSlots[sequenceNum] = &Instance{isDecided: true, AcceptorState: &newAcceptorState}
	}

	reply.Error = "OK"
	return nil
}

// Done phase for Acceptor
type DoneArgs struct {
	SequenceNum    int
	PeerIndex int
}

type DoneReply struct {
	Error string
}

func (px *Paxos) DoneHandler(args *DoneArgs, reply *DoneReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	sequenceNum, peerIndex := args.SequenceNum, args.PeerIndex
	if sequenceNum > px.DoneNumbersOfPaxosPeers[peerIndex] {
		px.DoneNumbersOfPaxosPeers[peerIndex] = sequenceNum
		minSequenceNumberToForget := px.Min()
		for sequenceNumber, instance := range px.InstanceSlots {
			if sequenceNumber < minSequenceNumberToForget && instance.isDecided {
				delete(px.InstanceSlots, sequenceNumber)
			}
		}
	}

	reply.Error = "OK"
	return nil
}

func (px *Paxos) Done(seq int) {
	if seq > px.DoneNumbersOfPaxosPeers[px.me] {
		for i, peer := range px.peers {
			args := DoneArgs{seq, px.me}
			var reply DoneReply
			if i == px.me {
				px.DoneHandler(&args, &reply)
			} else {
				ok := call(peer, "Paxos.DoneHandler", &args, &reply)
				if !ok {
					reply.Error = "NetworkError"
				}
			}
		}
	}
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	go func(sequenceNum int, value interface{}) {
		px.mu.Lock()
		instance, ok := px.InstanceSlots[sequenceNum]
		if ok && instance.isDecided {
			return
		} else if !ok {
			newAcceptorState := AcceptorState{PromiseNumber: -1, AcceptNumber:  -1, AcceptValue:   nil}
			px.InstanceSlots[sequenceNum] = &Instance{isDecided: false, AcceptorState: &newAcceptorState}
		}
		px.mu.Unlock()

		isDecided, _ := px.Status(sequenceNum)
		for proposalNum := -1; !px.isdead() && isDecided != Decided && sequenceNum >= px.Min(); isDecided, _ = px.Status(sequenceNum) {
			proposalNum = px.GetProposalNum(proposalNum)
			
			// Prepare phase
			var highestNProposed int = -1
			promiseValue := value
			prepareVote := 0
			for i, peer := range px.peers {
				args := PrepareArgs{sequenceNum, proposalNum}
				var reply PrepareReply

				if i == px.me {
					px.PrepareHandler(&args, &reply)
				} else {
					ok := call(peer, "Paxos.PrepareHandler", &args, &reply)
					// deal with network errors
					if !ok {
						reply.Error = "NetworkError"
					}
				}

				// Keep count of vote
				if reply.Error == "OK" {
					prepareVote++
					if reply.AcceptorState.AcceptNumber > highestNProposed {
						highestNProposed = reply.AcceptorState.AcceptNumber
						promiseValue = reply.AcceptorState.AcceptValue
					}
				}
			}

			// didn't get majority vote
			if prepareVote <= len(px.peers) / 2 {
				continue
			}

			// Accept Phase
			acceptVote := 0
			for i, peer := range px.peers {
				args := AcceptArgs{sequenceNum, proposalNum, promiseValue}
				var reply AcceptReply
				if i == px.me {
					px.AcceptHandler(&args, &reply)
				} else {
					ok := call(peer, "Paxos.AcceptHandler", &args, &reply)
					// deal with network errors
					if !ok {
						reply.Error = "NetworkError"
					}
				}

				// Keep count of vote
				if reply.Error == "OK" {
					acceptVote++
				}
			}

			// didn't get majority vote
			if acceptVote <= len(px.peers) / 2 {
				if highestNProposed > proposalNum {
					proposalNum = highestNProposed
				}
				continue
			}
			
			for i, peer := range px.peers {
				args := LearnArgs{sequenceNum, promiseValue}
				var reply LearnReply
				if i == px.me {
					px.LearnHandler(&args, &reply)
				} else {
					ok := call(peer, "Paxos.LearnHandler", &args, &reply)
					if !ok {
						reply.Error = "NetworkError"
					}
				}
			}
		}
	}(seq, v)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	max := -1
	for sequenceNum, _ := range px.InstanceSlots {
		if sequenceNum > max {
			max = sequenceNum
		}
	}
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefore cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	min := -1
	for _, ele := range px.DoneNumbersOfPaxosPeers {
		if min == -1 || ele < min {
			min = ele
		}
	}
	return min + 1
}


//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if seq < px.Min() {
		return Forgotten, nil
	}

	instance, ok := px.InstanceSlots[seq]
	if ok && instance.isDecided {
		return Decided, instance.AcceptorState.AcceptValue
	} else {
		return Pending, nil
	}
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me


	// Your initialization code here.
	px.InstanceSlots = make(map[int]*Instance)
	px.DoneNumbersOfPaxosPeers = make([]int, len(peers))
	for i, _ := range px.DoneNumbersOfPaxosPeers {
		px.DoneNumbersOfPaxosPeers[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) GetProposalNum(value int) int {
	proposalNum := value + 1
	for ;proposalNum % len(px.peers) != px.me; proposalNum++ {
	}
	return proposalNum
}