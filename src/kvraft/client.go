package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"
import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeaderId int   //lab3 要求不能每次都查找leader服务器，应该记录下来
	clientNo     int64 //客户端编号
	reqNo        int64 // 每次请求的编号
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientNo = nrand()

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) randServer() int {
	return int(nrand() % int64(len(ck.servers)))
}

func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := &Args{
		Op:  GET,
		Key: key,
	}
	reply := &Reply{}

	ck.sendRpc(args, reply)

	return reply.Value
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &Args{
		Op:    op,
		Key:   key,
		Value: value,
	}
	args.ReqNo = atomic.AddInt64(&ck.reqNo, 1)
	reply := &Reply{}

	ck.sendRpc(args, reply)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}

func (ck *Clerk) sendRpc(args *Args, reply *Reply) {
	DPrintf("client[%d]send rpc[%d]", ck.clientNo, ck.reqNo)
	defer DPrintf("client[%d]send rpc finish", ck.clientNo)

	index := ck.lastLeaderId
	args.ClientNo = ck.clientNo

	for {
		ok := ck.servers[index].Call(getRpcName(args.Op), args, reply)
		if ok && reply.Err == OK {
			ck.lastLeaderId = index
			break
		}

		time.Sleep(time.Millisecond * 10)
		index = (index + 1) % len(ck.servers)
		DPrintf("change kvserve ok[%v] , reply[%+v]" ,ok ,reply)
	}

}
