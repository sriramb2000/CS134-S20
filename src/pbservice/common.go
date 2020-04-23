package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	ErrBadRequest  = "ErrBadRequest"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	ID	  int64
	TxnType string
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
	BackupValue string
}

type GetArgs struct {
	Key   string
	// You'll have to add definitions here.
	ID	  int64
	TxnType string
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.
type SyncArgs struct {
	History	map[int64]TxnRecord
	DB	    map[string]string
}

type SyncReply struct {
	Err	  Err
}