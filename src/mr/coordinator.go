package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"
import "fmt"

//each assgin task 
const(
	SUSCCESS = iota
	NOT_FINISHED
	FINISHED
)

// task state
const(
	UNSSIGNED = iota
	RUNNING
	DONE
)

type Coordinator struct {
	// Your definitions here.
	done chan bool  // Done when  c <- nReduce
	lock sync.Mutex  // lock service
	mapTasks []Task // map tasks
	reduceTasks []Task // reduce tasks
	reduceDirs []string
	workers []Wker  // workers register
	workerId int  // sefl add to worker register
	worker_task_map map[int]*Task  // workerId to task
}

var c Coordinator

type Task struct {
	taskId int  // reduce task num
	filePath []string // task file path
	assignTime int64 // task assign time millisecond
	state int8 // 0: unassigned   1:running   2:finished
	workers []int // more worker do the same task
	taskType int8 // 1 map  2 reduce
}

func(t *Task) timeout(now int64) bool{
	return now - t.assignTime >= 10
}

// Your code here -- RPC handlers for the worker to call.

// init coordinator

func(c *Coordinator) init(files []string, nReduce int){
	
	//init member variables
	c.done = make(chan bool ,nReduce)
	c.mapTasks, c.reduceTasks = make([]Task ,0) ,make([]Task ,0)
	c.workers , c.worker_task_map = make([]Wker ,0)  , make(map[int]*Task, 0)
	c.reduceDirs = make([]string, 0)
	// init maptasks
	for i := 0 ; i < len(files) ; i ++ {
		t := Task{}
		t.taskId = i
		t.taskType = MAPTASK
		t.filePath = make([]string ,0)
		t.filePath = append(t.filePath , files[i])
		c.mapTasks = append(c.mapTasks , t)
	}

	//init reduceTasks
	for i := 0 ; i < nReduce ; i ++ {
		t := Task{}
		t.taskId = i
		t.workers = make([]int ,0)
		t.taskType = REDUCETASK
		c.reduceTasks = append(c.reduceTasks , t)
	}
	//fmt.Println(len(c.mapTasks) , len(c.reduceTasks))
}

// 1, Worker向 coordinator注册自己
func(c *Coordinator ) Register(args *Args , reply *RegisterReply) error {
	//fmt.Println("worker register")
	c.lock.Lock()
	c.workerId ++  // worker add 
	c.lock.Unlock()

	worker := Wker{}
	worker.preTime = time.Now().Unix()
	worker.workerId = c.workerId
	worker.state = 0

	c.workers = append(c.workers ,worker)

	reply.WorkerId = worker.workerId

	return nil
}

// 2, worker 询问是否有任务，如果有任务，则返回一个任务
func(t *Task ) assign(args *Args , reply *Reply , taskType int8){
	t.state = RUNNING
	t.assignTime = time.Now().Unix()
	t.workers = append (t.workers ,args.WorkerId)

	c.worker_task_map[args.WorkerId] = t

	reply.TaskType = t.taskType
	reply.NReduce = len(c.reduceTasks)

	if t.taskType == MAPTASK {
		reply.FilePath = append(reply.FilePath, t.filePath[0])
	}else if t.taskType == REDUCETASK {
		reply.FilePath = c.reduceDirs
		reply.ReduceTaskNum = t.taskId
	}
}

func(c *Coordinator ) assign( tasks []Task, args *Args , reply *Reply) int {
	// 0 assign successful  1 have task don't finished  2 all finished
	ret := FINISHED
	for i := 0 ; i <  len(tasks) ; i ++ {
		t := &tasks[i]
		if t.state == UNSSIGNED {
			t.assign(args ,reply,t.taskType)
			return SUSCCESS
		}else if t.state == RUNNING && t.timeout( time.Now().Unix() )  {
			t.assign(args ,reply,t.taskType)
			return SUSCCESS
		}else if t.state == RUNNING {
			ret = NOT_FINISHED
		}
		
	}
	return ret;
}

func(c *Coordinator ) AskTask(args *Args , reply *Reply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	state := c.assign( c.mapTasks ,args ,reply)

	if state != FINISHED { 
		return nil 
	}

	state = c.assign( c.reduceTasks ,args ,reply)

	return nil
}

// 3, 通知 coordinator任务完成
func(c *Coordinator ) FinishTask(args *Args , reply *Reply) error {

	c.lock.Lock()
	defer c.lock.Unlock()

	t := c.worker_task_map[ args.WorkerId ] 
	t.state = DONE

	if t.taskType == REDUCETASK {
		c.done <- true
		filePath := args.FilePath
		l ,r := len(filePath) - 1 , len(filePath)
		for filePath[l] != '\\' && filePath[l] != '/' {
			l --
		}
		os.Rename(filePath, "/home/zwz/go/6.824/src/main/mr-tmp/" + filePath[l:r])
	}else if t.taskType == MAPTASK{
		c.reduceDirs = append(c.reduceDirs ,args.FilePath)
	}

	return nil
}


// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	//fmt.Println("reduce task:",len(c.reduceTasks))
	for i := 0 ; i < len(c.reduceTasks) ; i ++ {
		<- c.done
		//fmt.Println("finished",i + 1)
	}
	fmt.Printf("")
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c = Coordinator{}
	
	// Your code here.
	c.init(files ,nReduce)

	c.server()
	return &c
}
