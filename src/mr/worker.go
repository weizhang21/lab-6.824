package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "time"
import "os"
import "strconv"
import "encoding/json"
import "sort"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Wker struct {
	workerId int
	preTime int64
	state int8 // 0:no work  1:running work
}

func( w *Wker) register() {
	args := Args{}
	reply := RegisterReply{}
	
	//fmt.Println("starting... register")
	call("Coordinator.Register", &args, &reply)
	w.workerId = reply.WorkerId

	//fmt.Println("register success")
}

func(w *Wker ) running(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string  ){

	for {
		args , reply := Args{} , Reply{}
		args.WorkerId = w.workerId	
		//拉取任务
		call("Coordinator.AskTask", &args, &reply)

		switch reply.TaskType {
			case NONTASK: 
				time.Sleep(time.Second * 2)
				break
			case MAPTASK:
				//fmt.Println(w.workerId , " maptask doing...")
				rootDir , err := doMap(reply.FilePath[0] ,mapf , reply.NReduce)
				if err != nil {
					fmt.Println(err)
					os.Exit(0)
				} 
				args.WorkerId = w.workerId
				args.FilePath = rootDir
				call("Coordinator.FinishTask", &args, &reply)
				break
			case REDUCETASK:
				//fmt.Println(w.workerId , " reducetask doing...") 	
				filePath , err := doReduce(reply.FilePath  ,reducef , reply.ReduceTaskNum )
				if err != nil {
					fmt.Println(err)
					os.Exit(0)
				}
				args.WorkerId = w.workerId
				args.FilePath = filePath
				call("Coordinator.FinishTask", &args, &reply)
				break
		}
		
	}
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func writeToFile(data []KeyValue ,  nReduce int) (string , error){

	//init 
	var kvs [][]KeyValue = make([][]KeyValue , nReduce)
	for i := 0 ; i < nReduce ; i ++ {
		kvs[i] = make([]KeyValue ,0)
	}

	for i := 0 ; i < len(data) ; i ++ {
		kv := data[i]
		idx := ihash(kv.Key) % nReduce
		kvs[idx] = append(kvs[idx] ,kv)
	}

	dirRoot, err := ioutil.TempDir("/var/tmp/", "tmp")
	if err != nil {
		fmt.Println(" create reduce tmp file" ,err)
		return "" , err
	}

	for i := 0 ; i < nReduce ; i ++ {
		file , err := os.Create(dirRoot + "/" + strconv.Itoa(i))
		encoder := json.NewEncoder(file)
		encoder.Encode(kvs[i])
		file.Close()
		
		if err != nil {
			return "" , err
		}
	}
	return dirRoot , nil
}

func doMap(filePath string ,mapf func(string, string) []KeyValue , nReduce int) (string ,error) {
	file, err := ioutil.ReadFile(filePath) // For read access.
				
	if err != nil {
		log.Fatal("readFile fail:", err)
		return "" ,err
	}
	
	contents := string(file)

	var kvs []KeyValue

	kvs = mapf(filePath ,contents)

	dirRoot , err := writeToFile(kvs ,nReduce)

	return dirRoot , err
}

func readFromFile(path string) []KeyValue{
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("readFile :" ,err)
	}

	var kvs []KeyValue = make([]KeyValue ,0)

	err = json.NewDecoder(file).Decode( &kvs )
	if err != nil {
		fmt.Println("json decode :", err )
	}
	return kvs 
}

func doReduce( filePath []string ,reducef func(string, []string) string  , reduceTaskNum int) ( string ,error) {
	var kv_map = make(map[string][]string,0)
	for i := 0; i < len(filePath) ; i ++ {
		path := filePath[i];
		kv := readFromFile(path + "/" + strconv.Itoa(reduceTaskNum))
		for j := 0 ; j < len(kv) ; j ++ {
			key , val := kv[j].Key ,kv[j].Value
			_, ok := kv_map[key]
			if !ok {
				kv_map[key] = make([]string ,0)
			}
			kv_map[key] = append(kv_map[key] ,val)
		}
	}

	var keys = make([]string, 0 ,len(kv_map))
	for k := range kv_map {
		keys = append(keys ,k)
	}
	sort.Strings(keys)

	path := "/var/tmp/mr-out-" + strconv.Itoa(reduceTaskNum) +".txt"
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0766)

	if err != nil {
		fmt.Println(err)
		return "", nil
	}
	defer file.Close()
	
	for i := 0 ; i < len(keys) ; i ++ {
		key := keys[i]
		value := reducef(key ,kv_map[key])
		file.WriteString(key +" " + value +"\n")
	}
	
	return file.Name() , nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker := Wker{}

	worker.register()
	
	worker.running(mapf ,reducef)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		os.Exit(0)
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	
	fmt.Println(err)
	return false
}
