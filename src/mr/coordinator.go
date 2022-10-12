package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "sort"

// import "6.824/mr"

type Coordinator struct {
	// Your definitions here.
	workers   	 []int
	files     	 []string
	mapfinish    []bool
	reducefinish []bool
	intermediate []KeyValue
	i,j          int
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


var map_mutex sync.Mutex
var intermediate_mutex sync.Mutex

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AllocateTask(mrargs *MrArgs, mrreply *MrReply) error {
	allocatesuccess := false
	for fileindex:= 0; fileindex < len(c.files) && allocatesuccess == false; fileindex++ {
		if c.mapfinish[fileindex] == false {
			mrreply.Filename = c.files[fileindex]
			mrreply.Fileindex= fileindex
			mrreply.Tasktype = TaskMap
			allocatesuccess = true
		}
	}

	if allocatesuccess == false {
		intermediate_mutex.Lock()
		c.j = c.i + 1
		for c.j < len(c.intermediate) && c.intermediate[c.j].Key == c.intermediate[c.i].Key {
			c.j++
		}
		values := []string{}
		for k := c.i; k < c.j; k++ {
			values = append(values, c.intermediate[k].Value)
		}
		mrreply.Tasktype = TaskReduce
		mrreply.Key = c.intermediate[c.i].Key
		mrreply.Values = values
		intermediate_mutex.Unlock()
	}

	fmt.Printf("\n")
	return nil
}

func (c *Coordinator) GetMapResult(mrargs *MrArgs, mrreply *MrReply) error {
	intermediate_mutex.Lock()
	c.intermediate = append(c.intermediate, mrargs.Kva...)
	c.mapfinish[mrargs.Fileindex] = true

	allmapfinished := true
	for i := 0; i < len(c.mapfinish); i ++ {
		if c.mapfinish[i] == false {
			allmapfinished = false
		}
	}

	if allmapfinished == true {
		sort.Sort(ByKey(c.intermediate))
	}
	intermediate_mutex.Unlock()
	return nil
}

func (c *Coordinator) GetReduceResult(mrargs *MrArgs, mrreply *MrReply) error {

	return nil
}
//
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
	fmt.Printf("[worker] sockname: %v\n", sockname)
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
	for _,mapfinished := range c.mapfinish {
		if mapfinished == false {
			ret = false
			break
		}
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	for _,file := range files {
		c.files = append(c.files, file)
		c.mapfinish = append(c.mapfinish, false)
	}

	for i := 0; i < nReduce; i++ {
		c.workers = append(c.workers, i)
	}
	
	fmt.Printf("%v files need to be processed\n", len(c.files))
	for _,file := range c.files {
		fmt.Printf("\t%v\n", file)
	}
	fmt.Printf("%v workers\n", len(c.workers))
	c.intermediate = []KeyValue{}
	c.i = 0
	c.j = 0
	c.server()
	return &c
}
