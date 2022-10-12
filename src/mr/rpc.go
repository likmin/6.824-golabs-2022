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
type TaskType int32
const (
	TaskMap TaskType = 0
	TaskReduce		  = 1
	TaskFinish		  = 2
)
type MrArgs struct {
	Kva      []KeyValue
	Filename   string
	Fileindex  int
	Key        string
	Output     string
}

type MrReply struct {
	Filename  string
	Fileindex int
	Tasktype  TaskType
	Key       string
	Values    []string
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
