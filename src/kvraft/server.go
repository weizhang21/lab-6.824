package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       string // "Put" or "Append"
	Key      string
	Value    string
	ClientNo int64
	OpId     int
	ReqId    int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplies    map[int64]int64 // 记录每个客户端的执行到的请求序列号
	opMap          map[int]chan Op
	db             map[string]string
}

func (kv *KVServer) Get(args *Args, reply *Reply) {
	// Your code here.
	op := Op{
		Op:       GET,
		Key:      args.Key,
		ClientNo: args.ClientNo,
		ReqId:    args.ReqNo,
	}

	DPrintf("server %d：args[%+v]", kv.me, args)
	start := time.Now().UnixNano()
	index, _, isLeader := kv.rf.Start(op)
	// 判断是否是Leader
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("server %d：start doing index[%d] args[%+v] ", kv.me, index, args)
	receiveOP := kv.waiting(index)
	DPrintf("server %d finish index[%d]：%+v cost %+vms",
		kv.me,index, args, (time.Now().UnixNano()-start)/1e6)

	reply.Err = OK
	if receiveOP != op {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.db[op.Key]
}

func (kv *KVServer) PutAppend(args *Args, reply *Reply) {
	// Your code here.
	op := Op{
		Op:       args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientNo: args.ClientNo,
		ReqId:    args.ReqNo,
	}

	DPrintf("server %d：args[%+v]", kv.me, args)
	start := time.Now().UnixNano()
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("server %d start doing index[%d]：args[%+v] ", kv.me,index, args)
	receiveOP := kv.waiting(index)
	DPrintf("server %d finish index[%d]：%+v cost %+vms",
		kv.me, index, args, (time.Now().UnixNano()-start)/1e6)

	reply.Err = OK
	if op != receiveOP {
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) getOPChannel(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _ , ok := kv.opMap[index]; !ok {
		kv.opMap[index] = make(chan Op , 10)
	}
	return kv.opMap[index]
}

func (kv *KVServer) waiting(index int) Op {
	DPrintf("server[%d]: index[%d] is waiting", kv.me, index)
	defer DPrintf("server[%d]: index[%d] is finish", kv.me, index)

	ch := kv.getOPChannel(index)
	defer func(){
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.opMap, index)
	}()

	select {
	case op := <-ch:
			return op
	case <-time.After(time.Millisecond * 500):
		return Op{}
	}
}

func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		DPrintf("server[%d]commit %+v", kv.me, m.CommandIndex)
		if m.CommandValid {
			op := m.Command.(Op)
			
			kv.mu.Lock()
			maxId , ok := kv.lastApplies[op.ClientNo]
			if !ok || op.ReqId > maxId {
				switch op.Op {
				case PUT:
					kv.db[op.Key] = op.Value
				case APPEND:
					kv.db[op.Key] += op.Value
				}
				kv.lastApplies[op.ClientNo] = op.ReqId
			}
			kv.mu.Unlock()
			
			ch := kv.getOPChannel(m.CommandIndex)
			ch <- op		
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastApplies = make(map[int64]int64)
	kv.opMap = make(map[int]chan Op)
	kv.db = make(map[string]string)

	go kv.applier()

	return kv
}
