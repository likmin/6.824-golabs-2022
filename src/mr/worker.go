package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	mrargs := MrArgs{}
	mrreply := MrReply{}
	// Your worker implementation here.
	RequestTask(&mrargs, &mrreply)
	mrargs.Fileindex = mrreply.Fileindex
	mrargs.Filename  = mrreply.Filename
	if mrreply.Tasktype == TaskMap {
		filename := mrreply.Filename
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		mrargs.Kva = mapf(filename, string(content))
		ReturnMapResult(&mrargs, &mrreply)
	} else if mrreply.Tasktype == TaskReduce {
		mrargs.Key    = mrreply.Key
		mrargs.Output = reducef(mrreply.Key, mrreply.Values)
		
	}

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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func RequestTask(mrargs *MrArgs, mrreply *MrReply) {
	mrargs.Kva = []KeyValue{}
	ok := call("Coordinator.AllocateTask", &mrargs, &mrreply)
	// fmt.Printf("mrreply.Filename %v; mrreply.Tasktype %v\n", mrreply.Filename, mrreply.Tasktype)
	// if mrreply.Tasktype == TaskMap
	fmt.Printf("[RequestTask]")
	if ok {
		fmt.Printf("mrreply.Filename %v; mrreply.Tasktype %v\n", mrreply.Filename, mrreply.Tasktype)	
	} else {
		fmt.Printf("call failed!\n")
	}
}

func ReturnMapResult(mrargs *MrArgs, mrreply *MrReply) {
	ok := call("Coordinator.GetMapResult", &mrargs, &mrreply)
	if ok {
		fmt.Printf("[ReturnMapResult] successed!\n")	
	} else {
		fmt.Printf("[ReturnMapResult] failed!\n")
	}
}

func ReturnReduceResult(mrargs *MrArgs, mrreply *MrReply) {
	ok := call("Coordinator.GetReduceResult", &mrargs, &mrreply)
	if ok {
		fmt.Printf("[ReturnReduceResult] successed!\n")	
	} else {
		fmt.Printf("[ReturnReduceResult] failed!\n")
	}
}
//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	// fmt.Printf("[worker] sockname: %v\n", sockname)
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
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
