package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

const (
	NONTASK = iota
	MAPTASK
	REDUCETASK
)

type Args struct {
	WorkerId int //发出请求的workerId
	FilePath string  // 文件位置

}

type RegisterReply struct {
	WorkerId int
}

type Reply struct {
	FilePath []string  //文件位置
	TaskType int8 // 表示任务的种类 ，map or reduce
	NReduce int // nums of reduce task
	ReduceTaskNum int
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
