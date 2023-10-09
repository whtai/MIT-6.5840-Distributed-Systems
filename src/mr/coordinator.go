package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
)

type taskState int8

const (
	taskStateIdle taskState = iota
	taskStateInProgress
	taskStateCompleted
)

type taskRecord struct {
	typeOfTask TaskType
	taskID     string
	taskNumber int
	inputFiles []string
	state      taskState
	deadline   time.Time

	mu sync.RWMutex
}

func (tr *taskRecord) appendInputFile(file string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.inputFiles = append(tr.inputFiles, file)
}

type Coordinator struct {
	// Your definitions here.
	totalMapTask        int
	totalReduceTask     int
	completedMapTask    int
	completedReduceTask int
	mu                  sync.RWMutex

	mapTasks               []*taskRecord
	reduceTasks            []*taskRecord
	tasksByID              map[string]*taskRecord
	reduceTasksByPartition map[int]*taskRecord
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestForTask(args *RequestForTaskArgs, RequestForTaskReply *RequestForTaskReply) error {
	var taskSource []*taskRecord
	if !c.isMapComplete() {
		taskSource = c.mapTasks // only try to assign map tasks
	} else if !c.isReduceComplete() {
		taskSource = c.reduceTasks // only try to assign reduce tasks
	}

	var assignTask *taskRecord
	for _, task := range taskSource {
		task.mu.Lock()
		if task.state == taskStateIdle {
			task.state = taskStateInProgress
			task.deadline = time.Now().Add(10 * time.Second)
			task.mu.Unlock()

			assignTask = task
			break
		}
		task.mu.Unlock()
	}

	if assignTask != nil {
		RequestForTaskReply.TaskID = assignTask.taskID
		RequestForTaskReply.TypeOfTask = assignTask.typeOfTask
		RequestForTaskReply.TaskNumber = assignTask.taskNumber
		RequestForTaskReply.NumReduce = c.totalReduceTask
		RequestForTaskReply.InputFiles = assignTask.inputFiles
	}
	return nil
}

func (c *Coordinator) NotifyTaskCompletion(args *NotifyTaskCompletionArgs, reply *NotifyTaskCompletionReply) error {
	task := c.tasksByID[args.TaskID]
	if task == nil {
		return nil
	}

	task.mu.Lock()
	if task.state == taskStateCompleted {
		// task has already been completed by some other worker, do nothing
		task.mu.Unlock()
		return nil
	}

	task.state = taskStateCompleted
	task.mu.Unlock()

	if task.typeOfTask == MapTask {
		for _, file := range args.OutputFiles {
			reduceTask := c.reduceTasksByPartition[file.Partition]
			if reduceTask == nil {
				continue
			}
			reduceTask.appendInputFile(file.FileName)
		}
	}

	c.increaseCompletionCount(task.typeOfTask)

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

func (c *Coordinator) ingestTasks(files []string, nReduce int) {
	c.totalMapTask = len(files)
	c.totalReduceTask = nReduce

	var allTasks []*taskRecord
	for i, file := range files {
		task := taskRecord{
			typeOfTask: MapTask,
			taskID:     uuid.NewV4().String(),
			taskNumber: i,
			inputFiles: []string{file},
			state:      taskStateIdle,
		}
		allTasks = append(allTasks, &task)
	}

	for i := 0; i < nReduce; i++ {
		task := taskRecord{
			typeOfTask: ReduceTask,
			taskID:     uuid.NewV4().String(),
			taskNumber: i,
			state:      taskStateIdle,
		}
		allTasks = append(allTasks, &task)
	}

	c.tasksByID = make(map[string]*taskRecord, len(allTasks))
	c.reduceTasksByPartition = make(map[int]*taskRecord, nReduce)
	for _, task := range allTasks {
		if task.typeOfTask == MapTask {
			c.mapTasks = append(c.mapTasks, task)
		} else {
			c.reduceTasks = append(c.reduceTasks, task)
			c.reduceTasksByPartition[task.taskNumber] = task
		}
		c.tasksByID[task.taskID] = task
	}
}

func (c *Coordinator) periodicallyResetFailedTask() {
	go func() {
		for t := range time.Tick(time.Second) {
			var checkTasks []*taskRecord
			if c.isMapComplete() {
				checkTasks = c.reduceTasks
			} else {
				checkTasks = c.mapTasks
			}
			for _, task := range checkTasks {
				task.mu.Lock()
				if task.state == taskStateInProgress && task.deadline.Before(t) {
					task.state = taskStateIdle
				}
				task.mu.Unlock()
			}
		}
	}()
}

func (c *Coordinator) isMapComplete() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.completedMapTask == c.totalMapTask
}

func (c *Coordinator) isReduceComplete() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.completedReduceTask == c.totalReduceTask
}

func (c *Coordinator) increaseCompletionCount(typ TaskType) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if typ == MapTask {
		c.completedMapTask += 1
	} else {
		c.completedReduceTask += 1
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.isReduceComplete()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.ingestTasks(files, nReduce)
	c.periodicallyResetFailedTask()

	c.server()
	return &c
}
