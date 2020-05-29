package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNoDB       = "ErrNoDB"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64
	DoneId int64
	ConfigNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Op  string
	Id int64
	DoneId int64
	ConfigNum int
}

type GetReply struct {
	Err   Err
	Value string
}

type DBSnapshotArgs struct {
	ConfigNum int
}

type DBSnapshotReply struct {
	Database    map[string]string
	OpHistory   map[int64]string
	Err         Err
}
