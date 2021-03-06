package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	Key		   string
	Value	   string
	TxnType     string
	Id         int64 // ID of current op
	DoneId     int64 // ID of previous client op (so it can be cleaned from opHistory)
	ConfigNum  int
}


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	mu2          sync.Mutex
	currSeqNum   int
	config       shardmaster.Config
	db           map[string]string
	dbSnapshots  map[int]Snapshot
	opHistory    map[int64]string
}

type Snapshot struct {
	db        map[string]string
	opHistory map[int64]string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.CorrectShardForKey(args.Key, args.ConfigNum) {
		//println("server config num", kv.config.Num)
		//println("client config num", args.ConfigNum)
		reply.Err = ErrWrongGroup
		return nil
	}
	if kv.IsDuplicateGet(args) {
		kv.FormatGetReply(args.Id, reply)
		return nil
	}
	proposalOp := Op{Key: args.Key, TxnType: args.Op, Id: args.Id, DoneId: args.DoneId, ConfigNum: args.ConfigNum} //TODO: May need to add Value prop here
	kv.PassOp(proposalOp)

	kv.FormatGetReply(args.Id, reply)
	return nil
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.CorrectShardForKey(args.Key, args.ConfigNum) {
		reply.Err = ErrWrongGroup
		return nil
	}

	if kv.IsDuplicatePutAppend(args) {
		kv.FormatPutAppendReply(args.Id, reply) // Will always be OK
		return nil
	}
	proposalOp := Op{Key: args.Key, Value: args.Value, TxnType: args.Op, Id: args.Id, DoneId: args.DoneId, ConfigNum: args.ConfigNum}
	kv.PassOp(proposalOp)

	kv.FormatPutAppendReply(args.Id, reply)
	return nil
}

// Pass a Proposal ;D
func (kv* ShardKV) PassOp(proposalOp Op) error {
	//kv.ForgetOp(proposalOp.DoneId)
	for {
		seqNum := kv.currSeqNum
		kv.currSeqNum++

		status, res := kv.px.Status(seqNum)

		var acceptedOp Op
		if status == paxos.Decided { // If this instance has already been decided, proceed with decided value
			acceptedOp = res.(Op)
		} else { // Otherwise propose our own Op
			acceptedOp = kv.Consensify(seqNum, proposalOp)
		}

		kv.CommitOp(acceptedOp)
		kv.px.Done(seqNum) // This Paxos Peer can safely forget about this instance now that the value has been committed

		if proposalOp.Id == acceptedOp.Id { // We can respond to client
			break
		}
	}
	return nil
}

// Runs Paxos until some Consensus Op is reached for the given sequence numebr
func (kv* ShardKV) Consensify(seq int, val Op) Op {
	kv.px.Start(seq, val)
	to := 10 * time.Millisecond
	for {
		status, value := kv.px.Status(seq)
		if status == paxos.Decided {
			return value.(Op) // cast to Op
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

func (kv* ShardKV) CorrectShardForKey(key string, configNum int)  bool {
	shard := key2shard(key)
	return kv.config.Num == configNum && kv.config.Shards[shard] == kv.gid
}

// Actually Executes the Request, and caches the response
func (kv* ShardKV) CommitOp(operation Op) {
	txnType := operation.TxnType
	if txnType == "Reconfigure" {
		kv.CommitReconfigure(operation)
	} else if txnType == "Get" {
		kv.CommitGet(operation)
	} else if txnType == "Put" {
		kv.CommitPut(operation)
	} else if txnType == "Append" {
		kv.CommitAppend(operation)
	}
}

func (kv *ShardKV) CommitGet(operation Op) {
	key, id, configNum := operation.Key, operation.Id, operation.ConfigNum
	curVal, ok := kv.db[key]
	if (!kv.IsDuplicateGet(&GetArgs{Id: id})) {
		if (kv.CorrectShardForKey(key, configNum)) {
			if ok {
				kv.opHistory[id] = curVal
			} else {
				kv.opHistory[id] = ErrNoKey
			}
		} else {
			kv.opHistory[id] = ErrWrongGroup
		}
	}
}

func (kv *ShardKV) CommitPut(operation Op) {
	key, val, id, configNum := operation.Key, operation.Value, operation.Id, operation.ConfigNum
	if (!kv.IsDuplicatePutAppend(&PutAppendArgs{Id: id})) {
		if (kv.CorrectShardForKey(key, configNum)) {
			kv.db[key] = val
			kv.opHistory[id] = OK
		} else {
			kv.opHistory[id] = ErrWrongGroup
		}
	}
}

func (kv *ShardKV) CommitAppend(operation Op) {
	key, val, id, configNum := operation.Key, operation.Value, operation.Id, operation.ConfigNum
	curVal, ok := kv.db[key]
	if (!kv.IsDuplicatePutAppend(&PutAppendArgs{Id: id})) {
		if (kv.CorrectShardForKey(key, configNum)) {
			if ok {
				kv.db[key] = curVal + val
				kv.opHistory[id] = OK
			} else {
				kv.db[key] = val
				kv.opHistory[id] = OK
			}
		} else {
			kv.opHistory[id] = ErrWrongGroup
		}
	}
}

func (kv *ShardKV) CommitReconfigure(operation Op) {
	id, latestConfigNum := operation.Id, operation.ConfigNum
	// update current DB until curr DB syncs with latest config
	if (!kv.IsDuplicateReconfig(id)){
		for kv.config.Num < latestConfigNum {
			if kv.config.Num == 0 {
				kv.config = kv.sm.Query(1)
			} else {
				//  Save the current DB as a snapshot
				currDBSnapshot := make(map[string]string)
				for k, v := range kv.db {
					if kv.config.Shards[key2shard(k)] == kv.gid {
						currDBSnapshot[k] = v
					}
				}
				currOpHistorySnapshot := make(map[int64]string)
				for k, v := range kv.opHistory {
					currOpHistorySnapshot[k] = v
				}
				kv.mu2.Lock()
				kv.dbSnapshots[kv.config.Num] = Snapshot{db: currDBSnapshot, opHistory: currOpHistorySnapshot}
				kv.mu2.Unlock()
	
				//  Update db to next config
				nextConfig := kv.sm.Query(kv.config.Num + 1)
				for shard, currGid := range kv.config.Shards {
					nextGid := nextConfig.Shards[shard]
					// if for the same shard, this shard is about to be served by me, then retrieve the data from its current server
					if currGid != nextGid && nextGid == kv.gid {
						done := false
						//  Get the DB snapshot and OpHistory from the current server
						for !done {
							for _, server := range kv.config.Groups[currGid] {
								args := &DBSnapshotArgs{kv.config.Num}
								var reply DBSnapshotReply
	
								// update DB and OpHistory
								ok := call(server, "ShardKV.GetDBSnapshotAndOpHistory", args, &reply)
								if ok && reply.Err == OK {
									for k, v := range reply.Database {
										kv.db[k] = v
									}
									for k, v := range reply.OpHistory {
										kv.opHistory[k] = v
									}
	
									done = true
									break
								}
							}
						}
					}
				}
	
				kv.config = nextConfig
			}
		}
	}
	
}

//
// UpdateDatabase RPC Handler
// Update current server's DB with shard contents from other servers
// for the specified viewnum
//
func (kv *ShardKV) GetDBSnapshotAndOpHistory(args *DBSnapshotArgs, reply *DBSnapshotReply) error {
	kv.mu2.Lock()
	defer kv.mu2.Unlock()

	if snapshot, ok := kv.dbSnapshots[args.ConfigNum]; ok {
		//reply.Database = make(map[string]string)
		//for k, v := range db {
		//	reply.Database[k] = v
		//}
		reply.Database = snapshot.db
		//reply.OpHistory = make(map[int64]string)
		//for k, v := range kv.opHistory {
		//	reply.OpHistory[k] = v
		//}
		reply.OpHistory = snapshot.opHistory
		reply.Err = OK
	} else {
		reply.Err = ErrNoDB
	}
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	newConfig := (kv.sm.Query(-1))
	if kv.config.Num != newConfig.Num {
		operation := Op{TxnType: "Reconfigure", ConfigNum: newConfig.Num, Id: nrand()}
		kv.PassOp(operation)
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.db = make(map[string]string)
	kv.dbSnapshots = make(map[int]Snapshot)
	kv.opHistory = make(map[int64]string)
	kv.currSeqNum = 0
	kv.config = kv.sm.Query(-1)

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}

// Assumes that another request with the same ID will not arrive before this request is cleared from history
func (kv *ShardKV) IsDuplicateReconfig(id int64) bool {
	res, ok := kv.opHistory[id]
	return ok && res == OK
}

// Assumes that another request with the same ID will not arrive before this request is cleared from history
func (kv *ShardKV) IsDuplicateGet(args *GetArgs) bool {
	id := args.Id
	res, ok := kv.opHistory[id]
	//println("duplicate get, key = ", args.Key, ", res = ", res)
	return ok && res != ErrWrongGroup
}

// Assumes that another request with the same ID will not arrive before this request is cleared from history
func (kv* ShardKV) IsDuplicatePutAppend(args *PutAppendArgs) bool {
	id := args.Id
	res, ok := kv.opHistory[id]

	return ok && res == OK
}

// Will format reply appropriately given the Request ID - Assumes that the correct response has already been cached
// in the `PassOp` phase
func (kv* ShardKV) FormatGetReply(id int64, reply *GetReply) {
	val := kv.opHistory[id]

	if val == ErrNoKey {
		reply.Value = ""
		reply.Err = ErrNoKey
	} else if val == ErrWrongGroup {
		reply.Value = ""
		reply.Err = ErrWrongGroup
	} else {
		reply.Value = val
		reply.Err = OK
	}
}

// Will format reply appropriately given the Request ID - Assumes that the correct response has already been cached
// in the `PassOp` phase
func (kv* ShardKV) FormatPutAppendReply(id int64, reply *PutAppendReply) {
	val, ok := kv.opHistory[id]
	if !ok || val == ErrWrongGroup {
		reply.Err = ErrWrongGroup
	} else if ok {
		reply.Err = OK
	}
}

// Clears request from Cache
//func (kv* ShardKV) ForgetOp(opId int64) {
//	if opId != -1 {
//		_, ok := kv.opHistory[opId]
//		if ok {
//			delete(kv.opHistory, opId)
//		}
//	}
//}