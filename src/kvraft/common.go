package kvraft

import "log"

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

type Err string

type Args struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientNo int64
	ReqNo    int64
}

type Reply struct {
	Err   Err
	Value string
}

func max(x, y int) int {
	if x >= y {
		return x
	}
	return y
}

func getRpcName(op string) string {
	var rpcName string
	switch op {
	case GET:
		rpcName = "KVServer.Get"
	case PUT:
		rpcName = "KVServer.PutAppend"
	case APPEND:
		rpcName = "KVServer.PutAppend"
	default:
		panic("op invalid")
	}
	return rpcName
}
