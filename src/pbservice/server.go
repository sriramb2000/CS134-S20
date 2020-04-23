package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type TxnRecord struct {
	key		   string
	value	   string
	txnType     string
}

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	currView   viewservice.View
	db		   map[string]string
	dbSyncFlag bool
	txnHistory map[int64]TxnRecord
}

// Helpers

func (pb *PBServer) IsPrimary() bool {
	return pb.currView.Primary == pb.me
}

func (pb *PBServer) IsBackup() bool {
	return pb.currView.Backup == pb.me
}

func (pb *PBServer) IsDuplicateGet(args *GetArgs) bool {
	id, key, txnType := args.ID, args.Key, args.TxnType
	record, ok := pb.txnHistory[id]

	if (ok && record.key == key && record.txnType == txnType) {
		return true
	}

	return false
}

func (pb *PBServer) IsDuplicatePutAppend(args *PutAppendArgs) bool {
	id, key, val, txnType := args.ID, args.Key, args.Value, args.TxnType
	record, ok := pb.txnHistory[id]

	if (ok && record.key == key && record.txnType == txnType) {
		return true
	}

	return false
}

func (pb *PBServer) ApplyGet(args *GetArgs, reply *GetReply) error {
	id, key, txnType := args.ID, args.Key, args.TxnType
	entry, ok := pb.db[key]

	//TODO: Should probably handle Bad GETs (where TxnType != "Get")

	if (ok) { // Value in DB
		reply.Value = entry
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrNoKey
	}

	// Value not in DB
	// Key note: Value here is not being used to store the request arguments
	// Instead it is being used to store the value that this particular get returned
	pb.txnHistory[id] = TxnRecord{key: key, value: entry, txnType: txnType}
	return nil
}

func (pb *PBServer) ApplyPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	id, key, val, txnType := args.ID, args.Key, args.Value, args.TxnType
	entry, ok := pb.db[key]

	reply.Err = OK

	if (txnType == "Put" || (txnType == "Append" && !ok)) { // If Put or Appending to new Key
		pb.db[key] = val
	} else if (txnType == "Append") {
		pb.db[key] = entry + val
	} else {
		reply.Err = ErrBadRequest
	}

	// Value not in DB
	pb.txnHistory[id] = TxnRecord{key: key, value: val, txnType: txnType}
	return nil
}

// RPCs

func (pb *PBServer) BackupGet(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// Double Check that this Server is the Primary
	if (!pb.IsBackup()) {
		reply.Err = ErrWrongServer
		return nil
	}

	// Check if request is duplicate - although duplicate requests won't be forwarded
	if (pb.IsDuplicateGet(args)) {
		record, _ = pb.txnHistory[args.ID]
		reply.Value = record.value
		reply.Err = OK
		return nil
	}

	// Apply Get
	pb.ApplyGet(args, reply)
	
	// Return
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// Double Check that this Server is the Primary
	if (!pb.IsPrimary()) {
		reply.Err = ErrWrongServer
		return nil
	}

	// Check if request is duplicate
	if (pb.IsDuplicateGet(args)) {
		record, _ = pb.txnHistory[args.ID]
		reply.Value = record.value
		reply.Err = OK
		return nil
	}

	// Apply Get
	pb.ApplyGet(args, reply)

	// Forward to Duplicate if possible
	if (pb.currView.Backup != "") {
		ok := call(pb.currView.Backup, "PBServer.BackupGet", args, &reply)
		// If failure or data inconsistency, re-sync necessary since either Backup has changed
		// or the Databases are out of sync (perhaps an undetected Backup restart)
		if (!ok || reply.Err == ErrWrongServer || reply.Value != pb.db[args.Key]) {
			pb.dbSyncFlag = true
		}
	}
	
	// Return
	return nil
}

func (pb *PBServer) BackupPutAppend(args *PutAppendArgs, reply *PutAppendReply, res *string) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// Double Check that this Server is the Primary
	if (!pb.IsBackup()) {
		reply.Err = ErrWrongServer
		return nil
	}

	// Check if request is duplicate - although duplicate requests won't be forwarded
	if (pb.IsDuplicatePutAppend(args)) {
		reply.Err = OK
		return nil
	}

	// Apply PutAppend
	pb.ApplyPutAppend(args, reply)

	res = pb.db[args.Key]
	
	// Return
	return nil
}



func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// Double Check that this Server is the Primary
	if (!pb.IsPrimary()) {
		reply.Err = ErrWrongServer
		return nil
	}

	// Check if request is duplicate
	if (pb.IsDuplicatePutAppend(args)) {
		reply.Err = OK
		return nil
	}

	// Apply PutAppend
	pb.ApplyPutAppend(args, reply)

	// TODO: Forward to Duplicate if possible
	if (pb.currView.Backup != "") {
		var res string
		ok := call(pb.currView.Backup, "PBServer.BackupPutAppend", args, &reply, &res)
		// If failure or data inconsistency, re-sync necessary since either Backup has changed
		// or the Databases are out of sync (perhaps an undetected Backup restart)
		if (!ok || reply.Err == ErrWrongServer || (reply.Err != OK && res != pb.db[args.Key])) {
			pb.dbSyncFlag = true
		}
	}

	// Return

	return nil
}

func (pb *PBServer) BackupSync(args *SyncArgs, reply *SyncReply) error {
	nextView, err := pb.vs.Ping(pb.currView.Viewnum)
	if (err != nil) {
		fmt.Errorf("Failed Ping(%v)", pb.currview.Viewnum)
	}

	pb.currView = nextView
	
	if (!pb.IsBackup()) {
		reply.Err = ErrWrongServer
		return nil
	}

	db, history := args.DB, args.History 

	pb.db = db
	pb.txnHistory = history

	reply.Err = OK

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	nextView, err := pb.vs.Ping(pb.currView.Viewnum)
	if (err != nil) {
		fmt.Errorf("Failed Ping(%v)", pb.currview.Viewnum)
	} else if (nextView.Viewnum == pb.currView.Viewnum) {
		log.Printf("Ping(%v) is up to date", pb.currView.Viewnum)
		return 
	}

	// Determine whether or not we need to do a DB Sync
	if (nextView.Primary == pb.me && nextView.Backup != "" && currView.Backup != nextView.Backup) {
		pb.dbSyncFlag = true
	}

	if (pb.dbSyncFlag && pb.currView.Backup != "") {
		// Do dbSync with new backup
		args := SyncArgs{DB: pb.db, History: pb.txnHistory}
		var reply SyncReply
		ok := call(pb.currView.Backup, "PBServer.BackupSync", &args, &reply)
		if (ok && reply.Err == OK) {
			pb.dbSyncFlag = true
		}
	}

	pb.currView = nextView
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.currView = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}
	pb.db = make(map[string]string)
	pb.dbSyncFlag = false
	pb.txnHistory = make(map[int64]TxnRecord)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
