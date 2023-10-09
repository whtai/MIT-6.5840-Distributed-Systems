package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

type TaskType int8

const (
	MapTask TaskType = iota
	ReduceTask
)

type RequestForTaskArgs struct{}

type RequestForTaskReply struct {
	TaskID     string
	TypeOfTask TaskType
	TaskNumber int
	NumReduce  int
	InputFiles []string
}

type OutputFile struct {
	Partition int
	FileName  string
}

type NotifyTaskCompletionArgs struct {
	TaskID      string
	OutputFiles []OutputFile
}

type NotifyTaskCompletionReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
