package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	// todo: you'll want to add field(s) to ViewServer in server.go in order to keep track of the most recent time at which the viewservice has heard a Ping from each server. Perhaps a map from server names to time.Time. You can find the current time with time.Now().
	// todo: add field(s) to ViewServer to keep track of the current view.
	firstTime bool
	stuck bool
	currView View
	nextView View
	latestAcked uint
	liveServers map[string]time.Time

}

//
// server Ping RPC handler.
//
// todo: the viewservice needs a way to detect that a primary or backup has failed and re-started. For example, the primary may crash and quickly restart without missing sending a single Ping.
// todo: there may be more than two servers sending Pings. The extra ones (beyond primary and backup) are volunteering to be backup if needed.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	pingServer, pingNum := args.Me, args.Viewnum

	if vs.stuck {
		reply.View = vs.currView
		return nil
	}

	if pingNum == 0 { // server restarted
		if vs.firstTime { // first time view server is initiated
			vs.nextView = View{Viewnum: 1, Primary: pingServer, Backup: ""}
			vs.latestAcked += 1
			vs.liveServers[pingServer] = time.Now()
			vs.firstTime = false
		} else {
			if _, exist := vs.liveServers[pingServer]; exist { // an existing server restarts, and it is before timeout
				if pingServer == vs.currView.Primary { // primary server restarts
					if vs.currView.Backup != "" { // there is backup server
						vs.nextView = View{Viewnum: vs.currView.Viewnum + 1, Primary: vs.currView.Backup, Backup: vs.currView.Primary}
					} else { // no backup
						vs.stuck = true
					}
				} else if pingServer == vs.currView.Backup { // backup restarts
					vs.nextView = View{Viewnum: vs.currView.Viewnum + 1, Primary: vs.currView.Primary, Backup: vs.currView.Backup}
				} // else case? idle restarts? nothing here
			} else { // the restarted server is a new server
				if vs.currView.Primary != "" && vs.currView.Backup == "" { // only a primary is now used, we can use the new server as backup
					vs.nextView = View{Viewnum: vs.currView.Viewnum + 1, Primary: vs.currView.Primary, Backup: pingServer}
					vs.liveServers[pingServer] = time.Now()
				} else if vs.currView.Primary != "" && vs.currView.Backup != "" { // already has primary and backup. add it as idle
					vs.liveServers[pingServer] = time.Now()
				} // else case? should not happen
			}
		}
	}

	if _, exist := vs.liveServers[pingServer]; exist { // we should only allow a server that once sent a ping(0) to join liveServers
		vs.liveServers[pingServer] = time.Now()
	}

	if _, exist := vs.liveServers[pingServer]; exist && pingServer == vs.currView.Primary && pingNum == vs.currView.Viewnum { // a live server && currView primary && viewNum match can ack
		vs.latestAcked = pingNum + 1
	}

	if vs.nextView.Viewnum <= vs.latestAcked {
		vs.currView = vs.nextView
	}

	println(vs.currView.Viewnum, vs.currView.Primary, vs.currView.Backup, vs.latestAcked)

	reply.View = vs.currView
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.currView
	return nil
}

func (vs *ViewServer) getIdle() string {
	for k, _ := range vs.liveServers {
		if k != vs.currView.Primary && k != vs.currView.Backup {
			return k
		}
	}
	return ""
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
// todo: your viewservice needs to make periodic decisions, for example to promote the backup if the viewservice has missed DeadPings pings from the primary. Add this code to the tick() function, which is called once per PingInterval.
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.stuck {
		return
	}

	primaryTime, hasPrimary := vs.liveServers[vs.currView.Primary]
	backupTime, hasBackup := vs.liveServers[vs.currView.Backup]

	currTime := time.Now()
	cutoffTime := PingInterval * DeadPings



	if (hasPrimary && !hasBackup && currTime.Sub(primaryTime) > cutoffTime) ||  (hasPrimary && hasBackup && currTime.Sub(primaryTime) > cutoffTime && currTime.Sub(backupTime) > cutoffTime) {
		// (there is only primary, no backup && primary timeout) or (both primary and backup timeout)
		vs.stuck = true
	} else if (hasPrimary && currTime.Sub(primaryTime) > cutoffTime) {
		// primary timeout, but has backup
		println("Primary", vs.currView.Primary, "timeout")
		vs.nextView = View{Viewnum: vs.currView.Viewnum + 1, Primary: vs.currView.Backup, Backup: ""}
		delete(vs.liveServers, vs.currView.Primary)
	} else if (hasBackup && currTime.Sub(backupTime) > cutoffTime) {
		// only backup timeout, but has primary
		vs.nextView = View{Viewnum: vs.currView.Viewnum + 1, Primary: vs.currView.Primary, Backup: ""}
		delete(vs.liveServers, vs.currView.Backup)
	}

	// check if idle timeout && add idle as backup
	idle := vs.getIdle()
	if idle != "" && currTime.Sub(vs.liveServers[idle]) > cutoffTime { // idle timeout
		delete(vs.liveServers, idle)
	}
	idle = vs.getIdle()
	if vs.nextView.Backup == "" && idle != "" { // no backup but has idle
		vs.nextView.Backup = idle
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.firstTime = true
	vs.stuck = false
	vs.currView = View{Viewnum: 0, Primary: "", Backup: ""}
	vs.nextView = View{Viewnum: 0, Primary: "", Backup: ""}
	vs.latestAcked = 0
	vs.liveServers = make(map[string]time.Time)
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
