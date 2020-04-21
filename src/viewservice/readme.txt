ver1:
view service:
1.
view struct{
    view number #;
    id(network port name);
    primary server; // must be primary or backup of previous view
    // except when first view, it can accept any server
    backup server(s); // ""(no backup) || any server(other than primary server)
}

servers[] server array;

server struct{
    id: string?;
    reStarted: bool;
    primary: bool;
    backup: bool;
}

2. for servers:
mainRoutine():
    while receive ping message from server:
        records the ping message (containing what the server currently know) ? is this necessary

        if ping message has 0 argument:
            this server just started/restarted

        records that the server is alive;
        replies with current view; // notice, server will not change current view until receive ping from primary backup

removeServer():
    while True:
        for each server:
            if has not received a ping from a server for more than DeadPings PingIntervals:
                server is considered dead

updateCurrentView():
    while True:
        if (both primary and backup are dead) || (any of primary or backup restarted) || (no backup and there is pinged server that are not primary nor backup):
            while until primary server sends a ping with current view number: // otherwise, must not do it
                change current view;

---
ver2:
view service:
1.
type View struct {
	Viewnum uint
	Primary string // must be primary or backup of previous view // except when first view, it can accept any server
	Backup  string // ""(no backup) || any server(other than primary server)
}

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	servers {serverId: time}
	currentView *view
	nextView *view
}

2. for servers:
Ping():
    // records the ping message (containing what the server currently know)
    if message from ViewServer.currentView.primary && ping message number X = ViewServer.currentView.viewNumber:
        currView = nextView;

    if ping message has 0 argument: //this server just started/restarted
        if server in servers: // this will not happen if timeout, because the server should have been removed by removeServer()
            // only when server restarts before timeout

        else:
            add it to server dict, with reStarted = True

    // records that the server is alive;
    update servers[id].lastPingMessage;

    replies with currentView; // notice, server will not change current view until receive ping from primary backup

updateCurrentView():
    // todo: logic is sloppy here
    while True:
        if (case1: both primary and backup are dead) || (case2: any of primary or backup restarted) || (case3: no backup and there is pinged server that are not primary nor backup):
            while True:
                if primary server sends a ping with current view number == True: // otherwise, must not do it
                    // change current view;


---
ver3:
type View struct {
	Viewnum uint
	Primary string // must be primary or backup of previous view // except when first view, it can accept any server
	Backup  string // ""(no backup) || any server(other than primary server)
}

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	servers {serverName: time}
	currView *view
	nextView *view
}

type server struct {
    name string
    lastPingTime time
    restarted bool
}


Ping():
    if ping message server just restarted:
        if ping message server in servers:
        // this will not happen if timeout, because the server should have been removed by updateNextView()
        // only when server restarts before timeout
            set server restarted = True
        else:
            add it to server dict
            set server restarted = True

    // records that the server is alive;
    update servers[id].lastPingMessage;

    if message is sent from primary server && message # == currView #:
        updateNextView();? not necessary
        mutex?
        currView = nextView;
        mutex?

    return currView;

updateNextView():
    lock?
    if any server timeout:
        remove that server from viewServer.servers
        if both are dead:
            nextView = null // stuck
        else if only primary timeout:
            nextView.primary = currView.backup
        else if only backup dead:
            nextView.backup = null

    if any of primary or backup restarted:
            if both are dead:
                nextView = null // stuck
            else if only primary restarted:
                nextView.primary = currView.backup
            else if only backup restarted:
                nextView.primary = currView.backup
    if no backup and there is pinged server that are not primary nor backup:
        add that server to viewServer
        update nextView
    unlock?

---
ver4:
type View struct {
	Viewnum uint
	Primary string // must be primary or backup of previous view // except when first view, it can accept any server
	Backup  string // ""(no backup) || any server(other than primary server)
}

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	stuck bool
	firstTime bool
	servers {serverName: time}
	currView *view
	nextView *view
}

type server struct {
    name string
    lastPingTime time
    restarted bool
}


Ping():
    if VS is stuck:
        return currView

    if ping server = 0:
        if first time VS starts:
            set currView primary
            add ping server to VS.servers
        else:
            if primary or backup or idle restarts before timeout:
                if it is primary:
                    if there is backup:
                        promote backup
                        make primary the backup
                    else if there is no backup:
                        VS.stuck = true
                if it is backup:
                    nothing much?
                if it is idle:
                    nothing much?
            else idle server first time joins:
                add it to server dict

    // records that the server is alive;
    update servers[id].lastPingMessage;

    if message is sent from currView primary server && message # == currView #:
        updateNextView();? not necessary
        mutex?
        currView = nextView;
        mutex?

    return currView;

updateNextView():
    lock?
    any server timeout:
        remove that server from viewServer.servers
        change currView primary
        if both are dead:
            nextView = null // stuck
        else if only primary timeout:
            nextView.primary = currView.backup
        else if only backup dead:
            nextView.backup = null

    any server restarted:
        if both are dead:
            nextView = null // stuck
        else if only primary restarted:
            nextView.primary = currView.backup
        else if only backup restarted:
            nextView.primary = currView.backup

    if no backup and there is pinged server that are not primary nor backup:
        add that server to viewServer
        update nextView
    unlock?

---
ver5:
type View struct {
	Viewnum uint
	Primary string // must be primary or backup of previous view // except when first view, it can accept any server
	Backup  string // ""(no backup) || any server(other than primary server)
}

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	stuck bool
	firstTime bool
	liveServers {serverName: time}
	snapshot *view
	view *view
}

Ping():
    if VS is stuck:
        return currView

    if ping server = 0:
        if first time VS starts:
            set view.primary
            add ping server to VS.servers
        else:
            if an existing primary or backup or idle restarts before timeout:
                if it is primary:
                    if there is backup:
                        promote backup to be view primary
                        demote primary to be view backup
                    else if there is no backup:
                        VS.stuck = true
                if it is backup:
                    nothing much?
                if it is idle:
                    nothing much?
            else idle server first time joins:
                add it to server dict

    // records that the server is alive;
    update servers[id].lastPingMessage;

    if message is sent from snapshot.primary server && message # == currView #:
        snapshot = view;

    return snapshot;

updateNextView():
    lock?
    any server timeout:
        remove that server from viewServer.servers
        change view primary
        if both are dead:
            nextView = null // stuck
        else if only primary timeout:
            if there is backup:
                promote backup to be view primary
                demote primary to be view backup
            else if there is no backup:
                VS.stuck = true
        else if only backup dead:
            nextView.backup = null

    if no backup and there is pinged server that are not primary nor backup:
        add that server to viewServer
        update nextView
    unlock?

---
ver6:
type View struct {
	Viewnum uint
	Primary string // must be primary or backup of previous view // except when first view, it can accept any server
	Backup  string // ""(no backup) || any server(other than primary server)
}

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	firstTime bool
	liveServers {serverName: time}
	snapshot *view
	view *view
}

Ping():
    if ping = 0:
        if first time VS starts:
            set view.primary
            add ping server to liveServers
        else:
            if an existing primary or backup or idle restarts before timeout:
                if it is primary:
                    if there is backup:
                        promote backup to be view primary
                        demote primary to be view backup
                    else if there is no backup:
                        VS.stuck = true
                if it is backup:
                    nothing much?
                if it is idle:
                    nothing much?
            else idle server first time joins:
                add it to liveServers

    // records that the server is alive;
    update servers[id].lastPingMessage;

    if primary is in liveServers && message is sent from snapshot.primary server && message # == currView #:
        snapshot = view;

    return snapshot;

updateNextView():
    lock?
    if (only primary && primary timeout) || (both primary and backup timeout):
        stuck
    else if only primary timeout:
        removePrimary
        set backup to be primary
    else if only backup timeout:
        remove backup
        updateView

    if no backup and there is pinged server that are not primary nor backup:
        add that server to viewServer
        update nextView
    unlock?

---
ver7:
type View struct {
	Viewnum uint
	Primary string // must be primary or backup of previous view // except when first view, it can accept any server
	Backup  string // ""(no backup) || any server(other than primary server)
}

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	firstTime bool
	stuck bool
	liveServers {serverName: time}
	snapshot *view
	view *view
}

addToLiveServers(server):
    add server:current time

Ping():
    if vs.stuck:
        return snapshot

    if ping = 0:
        if vs.firstTime:
            view = {N+1, pingServer, ""}
            snapshot = view
            addToLiveServers(pingServer)
            vs.firstTime = false
        else:
            if pingServer in liveServers: //an existing primary or backup or idle restarts before timeout:
                if pingServer == getPrimary():
                    if getBackup() != "": //there is backup:
                        view = {N+1, backup, primary}
                    else: // there is no backup:
                        vs.stuck = true
                if pingServer == getBackup():
                    view = {N+1, primary, backup}
                else: // idle server
                    nothing much?
            else server first time joins:
                if only primary:
                    view = {N+1, primary, backup}
                    addToLiveServers(pingServer)
                else if only primary and backup:
                    addToLiveServers(pingServer)
                else: // there are no more than 3 servers in test cases
                    should not happen

    // records that the server is alive;
    update servers[id].lastPingMessage;

    if pingServer in liveServers && message is sent from snapshot.primary server && message # == currView #:
        snapshot = view;

    return snapshot;

updateNextView():
    lock?
    if (only primary && primary timeout) || (both primary and backup timeout):
        stuck
    else if only primary timeout:
        removePrimary
        view = {N+1, backup, ""}
    else if only backup timeout:
        remove backup
        view = {N+1, backup, ""}

    if view has no backup and liveServer:
        view.backup = idle
    unlock?

---
ver8:
type View struct {
	Viewnum uint
	Primary string // must be primary or backup of previous view // except when first view, it can accept any server
	Backup  string // ""(no backup) || any server(other than primary server)
}

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	firstTime bool
	stuck bool
	liveServers {serverName: time}
	currView *view
	nextView *view
	latestAcked int
}

Ping():
    if vs.stuck:
        return currView

    if ping = 0: // there is a server restarted
        if vs.firstTime: // first time starts // test1
            nextView = {1, pingServer, ""}
            latestAcked += 1
            liveServers[pingServer] = ""
            vs.firstTime = false
        else:
            if pingServer in liveServers: //an existing primary or backup or idle restarts before timeout:
                if pingServer == currView.primary: // if primary restarts
                    if currView.backup != "": // if there is backup: // test6
                        nextView = {N+1, backup, primary}
                    else: // there is no backup:
                        vs.stuck = true
                if pingServer == getBackup(): // if backup restarts
                    nextView = {N+1, primary, backup}
                else: // idle server
                    nothing much?
            else server first time joins:
                if currView.primary != "" and currView.backup == "": // test2, test4
                    nextView = {N+1, currView.primary, pingServer}
                    liveServers[pingServer] = ""
                else if currView.primary != "" and currView.backup == "":
                    liveServers[pingServer] = ""
                else: // there are no more than 3 servers in test cases
                    should not happen

    // records that the server is alive;
    update servers[id].lastPingMessage;

    if pingServer in liveServers && message is sent from snapshot.primary server && message # == currView #:
        vs.latestAcked = ping + 1

    if nextView <= latestAcked:
        currView = deepcopies nextView

    return currView;

updateNextView():
    lock?
    if vs.stuck:
        return nil

    if (only primary && primary timeout) || (both primary and backup timeout):
        stuck
    else if only primary timeout: // test3, test5
        remove Primary from live servers
        nextView = {N+1, backup, ""}
    else if only backup timeout: // test7
        remove backup
        view = {N+1, backup, ""}

    if nextView has no backup and liveServer: // test5 // todo: must not change next view
        nextView.backup = idle

    unlock?