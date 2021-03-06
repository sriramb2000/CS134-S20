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
	Key		   string
	Value	   string
	TxnType     string
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
	smu		   sync.Mutex
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

	if (ok && record.Key == key && record.TxnType == txnType) {
		return true
	}

	return false
}


// Inspired by https://gist.github.com/sascha-andres/d1f11fb9bc6abc4f07b4118839b29d7f
func retry(attempts int, f func() error) error {
	if err := f(); err != nil {
		if attempts--; attempts > 0 {
		   log.Printf("Attempting again...")
		   return retry(attempts, f)
		}
		return err
	}
	return nil
}

func (pb *PBServer) SyncTilDead(pinged bool, nextView viewservice.View) error {
     	 return	retry(viewservice.DeadPings, func() error {
		var reply SyncReply
		pb.Sync(pinged, nextView, &reply)
		if (reply.Err == OK) {
			return nil
		} else if (reply.Err == ErrWrongServer) {
			reply.Err = ErrWrongServer
			return nil
		} else {
			return fmt.Errorf("ErrSyncFail")
		}
	})
}

func (pb *PBServer) IsDuplicatePutAppend(args *PutAppendArgs) bool {
	id, key, val, txnType := args.ID, args.Key, args.Value, args.TxnType
	record, ok := pb.txnHistory[id]

	if (ok && record.Key == key && record.Value == val && record.TxnType == txnType) {
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
	pb.txnHistory[id] = TxnRecord{Key: key, Value: entry, TxnType: txnType}
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
	}

	//else {
	//	reply.Err = ErrBadRequest
	//}

	// Value not in DB
	pb.txnHistory[id] = TxnRecord{Key: key, Value: val, TxnType: txnType}
	return nil
}

// RPCs

func (pb *PBServer) BackupGet(args *GetArgs, reply *GetReply) error {
     	//log.Printf("Backup Get for %s received.", args.Key)
	pb.mu.Lock()
	defer pb.mu.Unlock()
	
	// Double Check that this Server is the Primary
	if (!pb.IsBackup()) {
		reply.Err = ErrWrongServer
		return nil
	}

	// Check if request is duplicate - although duplicate requests won't be forwarded
	if (pb.IsDuplicateGet(args)) {
		record, _ := pb.txnHistory[args.ID]
		reply.Value = record.Value
		reply.Err = OK
		return nil
	}

	// Apply Get
	pb.ApplyGet(args, reply)
	
	// Return
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
     	// log.Printf("Get for %s received.", args.Key)
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
		record, _ := pb.txnHistory[args.ID]
		reply.Value = record.Value
		reply.Err = OK
		return nil
	}

	// Apply Get
	pb.ApplyGet(args, reply)

	// Forward to Duplicate if possible
	if (pb.currView.Backup != "") {
		// Retry until assumed dead, then set Sync flag
		for i := 0; i < viewservice.DeadPings; i += 1 {
			ok := call(pb.currView.Backup, "PBServer.BackupGet", args, reply)
			if (ok && reply.Value == pb.db[args.Key]) {
				break
			} else if (ok && (reply.Err == ErrWrongServer || reply.Value != pb.db[args.Key])) {
				// If failure or data inconsistency, re-sync necessary since either Backup has changed
				// or the Databases are out of sync (perhaps an undetected Backup restart)
				pb.dbSyncFlag = true
				break
			}
		}

		// If backup failed, must perform a DB transfer before committing
		if (pb.dbSyncFlag) {
			pb.SyncTilDead(false, pb.currView)
		}
	}
	
	// Return
	return nil
}

func (pb *PBServer) BackupPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
   	// log.Printf("Backup Put for %s,%s received.", args.Key, args.Value)
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// Your code here.

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

	reply.BackupValue = pb.db[args.Key]
	
	// Return
	return nil
}



func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
     	// log.Printf("Put for %s,%s received.", args.Key, args.Value)
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

	if (pb.currView.Backup != "") {
		// Retry until assumed dead, then set Sync flag
		for i := 0; i < viewservice.DeadPings; i += 1 {
			ok := call(pb.currView.Backup, "PBServer.BackupPutAppend", args, reply)
			if (ok && reply.Err == OK && reply.BackupValue == pb.db[args.Key]) {
				break
			} else if (ok && (reply.Err == ErrWrongServer || reply.BackupValue != pb.db[args.Key])) {
				// If failure or data inconsistency, re-sync necessary since either Backup has changed
				// or the Databases are out of sync (perhaps an undetected Backup restart)
				pb.dbSyncFlag = true
				break
			}
		}
		// If backup failed, must perform a DB transfer before committing
		 if (pb.dbSyncFlag) {
			pb.SyncTilDead(false, pb.currView)
		 }
		
	}

	// Return

	return nil
}

func (pb *PBServer) Sync(pinged bool, nextView viewservice.View, reply *SyncReply) error {
	pb.smu.Lock()
	defer pb.smu.Unlock()
	log.Printf("Syncing...")
	if (!pinged) {
		newView, err := pb.vs.Ping(pb.currView.Viewnum)
		if (err != nil) {
			fmt.Errorf("Failed Ping(%v)", pb.currView.Viewnum)
		} else {
		        nextView = newView
		}
		if (nextView.Primary == pb.me && nextView.Backup != "" && pb.currView.Backup != nextView.Backup) {
		        pb.dbSyncFlag = true
		}

	}

	if (nextView.Primary != pb.me) {
		reply.Err = ErrWrongServer
		log.Printf("Sync Failure Wrong Server")		
		return nil
	}
	
	if (pb.dbSyncFlag && nextView.Backup != "") {
		// Do dbSync with new backup
		args := SyncArgs{DB: pb.db, History: pb.txnHistory}
		// var reply SyncReply
		ok := call(nextView.Backup, "PBServer.BackupSync", &args, &reply)
		if (ok && reply.Err == OK) {
			pb.dbSyncFlag = false
			// reply.Err = OK
			log.Printf("Sync Success")
		} else {
			log.Printf("Sync Failure")
			reply.Err = ErrSyncFail
		}
	} else {
  		log.Printf("Sync Unnecessary No Backup")		
		reply.Err = OK
	}

	if (!pinged) {
		pb.currView = nextView
	}

	return nil
}

func (pb *PBServer) BackupSync(args *SyncArgs, reply *SyncReply) error {
 	pb.mu.Lock()
	defer pb.mu.Unlock()
	nextView, err := pb.vs.Ping(pb.currView.Viewnum)
	if (err != nil) {
		fmt.Errorf("Failed Ping(%v)", pb.currView.Viewnum)
	}

	pb.currView = nextView
	
	if (!pb.IsBackup()) {
		reply.Err = ErrWrongServer
		return nil
	}
	log.Println("Valid Backup Sync received.")

	db, history := args.DB, args.History 

	pb.db = db
	pb.txnHistory = history

	log.Println("Sync success.")
	
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
		fmt.Errorf("Failed Ping(%v)", pb.currView.Viewnum)
	} else if (nextView.Viewnum == pb.currView.Viewnum) {
		//log.Printf("Ping(%v) is up to date", pb.currView.Viewnum)
		if (pb.me == nextView.Primary) {
		   log.Printf("Primary: Ping(%v) is up to date", pb.currView.Viewnum)
		} else if (pb.me == nextView.Backup){
		   log.Printf("Backup: Ping(%v) is up to date", pb.currView.Viewnum)
		}
		return 
	}

	// Determine whether or not we need to do a DB Sync
	if (nextView.Primary == pb.me && nextView.Backup != "" && pb.currView.Backup != nextView.Backup) {
		pb.dbSyncFlag = true
	}

	// Sync
	pb.SyncTilDead(true, nextView)
	

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
