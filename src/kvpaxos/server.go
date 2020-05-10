// package kvpaxos

// import "net"
// import "fmt"
// import "net/rpc"
// import "log"
// import "paxos"
// import "sync"
// import "sync/atomic"
// import "os"
// import "syscall"
// import "encoding/gob"
// import "math/rand"
// import "time"


// const Debug = 0

// func DPrintf(format string, a ...interface{}) (n int, err error) {
// 	if Debug > 0 {
// 		log.Printf(format, a...)
// 	}
// 	return
// }


// type Op struct {
// 	// Your definitions here.
// 	// Field names must start with capital letters,
// 	// otherwise RPC will break.
// 	Key		   string
// 	Value	   string
// 	TxnType     string
// 	Id         int64 // ID of current op
// 	DoneId     int64 // ID of previous client op (so it can be cleaned from opHistory)
// }

// type KVPaxos struct {
// 	mu         sync.Mutex
// 	l          net.Listener
// 	me         int
// 	dead       int32 // for testing
// 	unreliable int32 // for testing
// 	px         *paxos.Paxos

// 	// Your definitions here.
// 	currSeqNum int 
// 	db         map[string]string
// 	opHistory  map[int64]string
// }

// // Helpers

// // Runs Paxos until some Consensus Op is reached for the given sequence numebr
// func (kv* KVPaxos) Consensify(seq int, val Op) Op {
// 	kv.px.Start(seq, val)

// 	to := 10 * time.Millisecond
// 	for {
// 		status, value := kv.px.Status(seq)
// 		if status == paxos.Decided {
// 			return value.(Op) // cast to Op
// 		}
// 		time.Sleep(to)
// 		if to < 10 * time.Second {
// 			to *= 2
// 		}
// 	}
// }

// // Pass a Proposal ;D
// func (kv* KVPaxos) PassOp(proposalOp Op) error {
// 	for {
// 		seqNum := kv.currSeqNum
// 		kv.currSeqNum++

// 		status, res := kv.px.Status(seqNum)

// 		var acceptedOp Op
// 		if status == paxos.Decided { // If this instance has already been decided, proceed with decided value
// 			acceptedOp = res.(Op)
// 		} else { // Otherwise propose our own Op
// 			acceptedOp = kv.Consensify(seqNum, proposalOp)
// 		}
		
// 		kv.ForgetOp(acceptedOp.DoneId)
// 		kv.CommitOp(acceptedOp)
// 		kv.px.Done(seqNum) // This Paxos Peer can safely forget about this instance now that the value has been committed

// 		if proposalOp.Id == acceptedOp.Id { // We can respond to client
// 			break
// 		}
// 	}
// 	return nil
// }

// // Clears request from Cache
// func (kv* KVPaxos) ForgetOp(opId int64) {
// 	if opId != -1 {
// 		_, ok := kv.opHistory[opId]
// 		if ok {
// 			delete(kv.opHistory, opId)
// 		}
// 	}
// }

// // Actually Executes the Request, and caches the response
// func (kv* KVPaxos) CommitOp(operation Op) {
// 	key, val, txnType, id := operation.Key, operation.Value, operation.TxnType, operation.Id
// 	curVal, ok := kv.db[key]
// 	var res string
// 	if txnType == "Get" {
// 		if ok {
// 			res = curVal
// 		} else {
// 			res = ErrNoKey
// 		}
// 	} else if txnType == "Put" {
// 		kv.db[key] = val
// 		res = OK
// 	} else if txnType == "Append" {
// 		if ok {
// 			kv.db[key] = curVal + val
// 		} else {
// 			kv.db[key] = val
// 		}
// 		res = OK
// 	}
// 	kv.opHistory[id] = res
// }

// // Assumes that another request with the same ID will not arrive before this request is cleared from history
// func (kv *KVPaxos) IsDuplicateGet(args *GetArgs) bool {
// 	id := args.Id
// 	_, ok := kv.opHistory[id]

// 	return ok 
// }

// // Assumes that another request with the same ID will not arrive before this request is cleared from history
// func (kv* KVPaxos) IsDuplicatePutAppend(args *PutAppendArgs) bool {
// 	id := args.Id
// 	res, ok := kv.opHistory[id]

// 	return ok && res == OK
// }

// // Will format reply appropriately given the Request ID - Assumes that the correct response has already been cached
// // in the `PassOp` phase
// func (kv* KVPaxos) FormatGetReply(id int64, reply *GetReply) {
// 	val := kv.opHistory[id]
// 	if val == ErrNoKey {
// 		reply.Value = ""
// 		reply.Err = ErrNoKey
// 	} else {
// 		reply.Value = val
// 		reply.Err = OK
// 	}
// }

// // Will format reply appropriately given the Request ID - Assumes that the correct response has already been cached
// // in the `PassOp` phase
// func (kv* KVPaxos) FormatPutAppendReply(id int64, reply *PutAppendReply) {
// 	_, ok := kv.opHistory[id]
// 	if ok {
// 	   reply.Err = OK
// 	}

// }

// func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
// 	// Your code here.
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// 	if kv.IsDuplicateGet(args) {
// 		kv.FormatGetReply(args.Id, reply)
// 		return nil
// 	}
// 	proposalOp := Op{Key: args.Key, TxnType: args.Op, Id: args.Id, DoneId: args.DoneId} //TODO: May need to add Value prop here
// 	kv.PassOp(proposalOp)

// 	kv.FormatGetReply(args.Id, reply)
// 	return nil
// }

// func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
// 	// Your code here.
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// 	if kv.IsDuplicatePutAppend(args) {
// 		kv.FormatPutAppendReply(args.Id, reply) // Will always be OK
// 		return nil
// 	}
// 	proposalOp := Op{Key: args.Key, Value: args.Value, TxnType: args.Op, Id: args.Id, DoneId: args.DoneId}
// 	kv.PassOp(proposalOp)
	
// 	kv.FormatPutAppendReply(args.Id, reply)
// 	return nil
// }

// // tell the server to shut itself down.
// // please do not change these two functions.
// func (kv *KVPaxos) kill() {
// 	DPrintf("Kill(%d): die\n", kv.me)
// 	atomic.StoreInt32(&kv.dead, 1)
// 	kv.l.Close()
// 	kv.px.Kill()
// }

// // call this to find out if the server is dead.
// func (kv *KVPaxos) isdead() bool {
// 	return atomic.LoadInt32(&kv.dead) != 0
// }

// // please do not change these two functions.
// func (kv *KVPaxos) setunreliable(what bool) {
// 	if what {
// 		atomic.StoreInt32(&kv.unreliable, 1)
// 	} else {
// 		atomic.StoreInt32(&kv.unreliable, 0)
// 	}
// }

// func (kv *KVPaxos) isunreliable() bool {
// 	return atomic.LoadInt32(&kv.unreliable) != 0
// }

// //
// // servers[] contains the ports of the set of
// // servers that will cooperate via Paxos to
// // form the fault-tolerant key/value service.
// // me is the index of the current server in servers[].
// //
// func StartServer(servers []string, me int) *KVPaxos {
// 	// call gob.Register on structures you want
// 	// Go's RPC library to marshall/unmarshall.
// 	gob.Register(Op{})

// 	kv := new(KVPaxos)
// 	kv.me = me

// 	// Your initialization code here.
// 	kv.db = make(map[string]string)
// 	kv.opHistory = make(map[int64]string)
// 	kv.currSeqNum = 0

// 	rpcs := rpc.NewServer()
// 	rpcs.Register(kv)

// 	kv.px = paxos.Make(servers, me, rpcs)

// 	os.Remove(servers[me])
// 	l, e := net.Listen("unix", servers[me])
// 	if e != nil {
// 		log.Fatal("listen error: ", e)
// 	}
// 	kv.l = l


// 	// please do not change any of the following code,
// 	// or do anything to subvert it.

// 	go func() {
// 		for kv.isdead() == false {
// 			conn, err := kv.l.Accept()
// 			if err == nil && kv.isdead() == false {
// 				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
// 					// discard the request.
// 					conn.Close()
// 				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
// 					// process the request but force discard of reply.
// 					c1 := conn.(*net.UnixConn)
// 					f, _ := c1.File()
// 					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
// 					if err != nil {
// 						fmt.Printf("shutdown: %v\n", err)
// 					}
// 					go rpcs.ServeConn(conn)
// 				} else {
// 					go rpcs.ServeConn(conn)
// 				}
// 			} else if err == nil {
// 				conn.Close()
// 			}
// 			if err != nil && kv.isdead() == false {
// 				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
// 				kv.kill()
// 			}
// 		}
// 	}()

// 	return kv
// }
package kvpaxos

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
import "time"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	Key       string
	Value     string
	Operation string
	CurrId    int64   //  Processing the current client request
	PrevId    int64   //  Cleaning up client requests previously served

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	currSeq      int                //  keeps track of current sequence instance
	database     map[string]string  //  keeps track of the k/v database
	prevRequests map[int64]string   //  keeps track of requests sent by the client
}


//
// Run Paxos on this instance until we have gained consensus
//
func RunPaxos(kv *KVPaxos, seq int, v Op) Op {
	kv.px.Start(seq, v)

	to := 10 * time.Millisecond
	for {
		operationStatus, operationValue := kv.px.Status(seq)
		if operationStatus == paxos.Decided {
			return operationValue.(Op)
		}

		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}


//
// Clean up client requests that have previously been served
//
func FreePrevRequest(kv *KVPaxos, prevId int64) {
	if prevId != -1 {
		_, ok := kv.prevRequests[prevId]
		if ok {
			delete(kv.prevRequests, prevId)
		}
	}
}


//
// The Paxos replicas have agreed on the order to apply this client request, so update the k/v database
//
func ApplyOperation(kv *KVPaxos, operationResult Op) {
	key, value, currId, operation := operationResult.Key, operationResult.Value, operationResult.CurrId, operationResult.Operation
	prev, ok := kv.database[key]
	//  Apply GET
	if operation == "Get" {
		if ok {
			kv.prevRequests[currId] = prev
		} else {
			kv.prevRequests[currId] = ErrNoKey
		}
	//  Apply PUT
	} else if operation == "Put" {
		kv.database[key] = value
		kv.prevRequests[currId] = OK
	//  Apply APPEND
	} else if operation == "Append" {
		kv.database[key] = prev + value
		kv.prevRequests[currId] = OK
	}
}


//
// Checks to see if this is a duplicate GET request to ensure at-most-once semantics
//
func IsDupGet(kv *KVPaxos, args *GetArgs) bool {
	key, currId := args.Key, args.CurrId
	prev, ok := kv.prevRequests[currId]

	//  Duplicate RPC request
	if ok && prev == key {
		return true
	}

	return false
}


//
// GET request handler
//
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//  Check if we have seen this request before, and if so, return stored value
	if IsDupGet(kv, args) {
		reply.Value = kv.database[args.Key]
		reply.Err = OK
		return nil
	}

	//  Try to assign the next available Paxos instance (sequence number) to each incoming client RPC
	for {
		//  Prepare Paxos value
		operation := Op{Key: args.Key, Value: "", Operation: "Get", CurrId: args.CurrId, PrevId: args.PrevId}
		currSeq := kv.currSeq
		kv.currSeq++

		//  Gain consensus on this Paxos instance
		var operationResult Op
		operationStatus, operationValue := kv.px.Status(currSeq)
		//  We are decided on this instance
		if operationStatus == paxos.Decided {
			operationResult = operationValue.(Op)
		//  We aren't decided on this instance, run Paxos until we are
		} else {
			operationResult = RunPaxos(kv, currSeq, operation)
		}

		//  Clean up client requests that have previously been served
		FreePrevRequest(kv, args.PrevId)
		//  We have agreed on this instance, so update database, and remember the request we served
		ApplyOperation(kv, operationResult)
		//  We are done processing this instance and will no longer need it or any previous instance
		kv.px.Done(currSeq)

		//  Paxos elected the current operation, so return GET result, done
		if operationResult.CurrId == args.CurrId {
			val := kv.prevRequests[args.CurrId]
			if val == ErrNoKey {
				reply.Value = ""
				reply.Err = ErrNoKey
			} else {
				reply.Value = kv.prevRequests[args.CurrId]
				reply.Err = OK
			}

			break
		}
	}

	return nil
}


//
// Checks to see if this is a duplicate PUT/APPEND request to ensure at-most-once semantics
//
func IsDupPutAppend(kv *KVPaxos, args *PutAppendArgs) bool {
	_, ok := kv.prevRequests[args.CurrId]

	//  Duplicate RPC request
	if ok {
		return true
	}

	return false
}


//
// PUT/APPEND request handler
//
func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//  Check if we have seen this request before
	if IsDupPutAppend(kv, args) {
		reply.Err = OK
		return nil
	}

	//  Try to assign the next available Paxos instance (sequence number) to each incoming client RPC
	for {
		//  Prepare Paxos value
		operation := Op{Key: args.Key, Value: args.Value, Operation: args.Op, CurrId: args.CurrId, PrevId: args.PrevId}
		currSeq := kv.currSeq
		kv.currSeq++

		//  Gain consensus on this Paxos instance
		var operationResult Op
		operationStatus, operationValue := kv.px.Status(currSeq)
		//  We are decided on this instance
		if operationStatus == paxos.Decided {
			operationResult = operationValue.(Op)
		//  We aren't decided on this instance, run Paxos until we are
		} else {
			operationResult = RunPaxos(kv, currSeq, operation)
		}

		//  Clean up client requests that have previously been served
		FreePrevRequest(kv, args.PrevId)
		//  We have agreed on this instance, so update database, and remember the request we served
		ApplyOperation(kv, operationResult)
		//  We are done processing this instance and will no longer need it or any previous instance
		kv.px.Done(currSeq)

		//  Paxos elected the current operation, so done
		if operationResult.CurrId == args.CurrId {
			break
		}
	}

	reply.Err = OK
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.currSeq = 0
	kv.database = make(map[string]string)
	kv.prevRequests = make(map[int64]string)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
