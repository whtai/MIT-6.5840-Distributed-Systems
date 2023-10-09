package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

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

	for {
		task, err := requestForTask(RequestForTaskArgs{})
		if err != nil {
			// assume coordinator has exited because the job is done, so the worker can terminate too
			// TODO: check for finer error
			break
		}

		if task.TaskID == "" {
			// no task assigned by coordinator, wait a bit then try again
			time.Sleep(1 * time.Second)
			continue
		}

		// handle task
		var outputFiles []OutputFile
		switch task.TypeOfTask {
		case MapTask:
			outputFiles, err = processMapTask(task.TaskNumber, task.NumReduce, task.InputFiles[0], mapf)
		case ReduceTask:
			outputFiles, err = processReduceTask(task.TaskNumber, task.InputFiles, reducef)
		}
		if err != nil {
			// assume this machine is broken
			break
		}

		// when task completes, notify coordinator
		_, err = notifyTaskCompletion(NotifyTaskCompletionArgs{
			TaskID:      task.TaskID,
			OutputFiles: outputFiles,
		})
		if err != nil {
			break
		}
	}
}

func requestForTask(args RequestForTaskArgs) (RequestForTaskReply, error) {
	var reply RequestForTaskReply
	err := call("Coordinator.RequestForTask", &args, &reply)
	return reply, err
}

func notifyTaskCompletion(args NotifyTaskCompletionArgs) (NotifyTaskCompletionReply, error) {
	var reply NotifyTaskCompletionReply
	err := call("Coordinator.NotifyTaskCompletion", &args, &reply)
	return reply, err
}

func processMapTask(taskNumber, nReduce int, inputFile string, mapf func(string, string) []KeyValue) ([]OutputFile, error) {
	// read from input file
	ifile, err := os.Open(inputFile)
	if err != nil {
		return nil, fmt.Errorf("cannot open %v", inputFile)
	}
	content, err := ioutil.ReadAll(ifile)
	if err != nil {
		return nil, fmt.Errorf("cannot read %v", inputFile)
	}
	ifile.Close()

	// call map function
	intermediate := mapf(inputFile, string(content))

	// write results to tmp files
	type mapOutputFile struct {
		name string
		file *os.File
	}
	tmpFiles := make(map[int]mapOutputFile) // partition -> file
	for _, ele := range intermediate {
		partition := ihash(ele.Key) % nReduce
		tmpFile, ok := tmpFiles[partition]
		if !ok {
			tmpFileName := fmt.Sprintf("tmp-%v-mr-%d-%d", randomString(5), taskNumber, partition)
			f, err := os.Create(tmpFileName)
			if err != nil {
				return nil, fmt.Errorf("cannot create %v", tmpFileName)
			}
			tmpFile = mapOutputFile{
				name: tmpFileName,
				file: f,
			}
			tmpFiles[partition] = tmpFile
		}

		enc := json.NewEncoder(tmpFile.file)
		if err := enc.Encode(ele); err != nil {
			return nil, fmt.Errorf("cannot encode %v", ele)
		}
	}

	// rename output files
	var outputFiles []OutputFile
	for k, v := range tmpFiles {
		_ = v.file.Close()
		newName := fmt.Sprintf("mr-%d-%d", taskNumber, k)
		if err := os.Rename(v.name, newName); err != nil {
			return nil, fmt.Errorf("cannot rename %v to %v", v.name, newName)
		}

		outputFiles = append(outputFiles, OutputFile{
			Partition: k,
			FileName:  newName,
		})
	}

	return outputFiles, nil
}

func processReduceTask(taskNumber int, inputFiles []string, reducef func(string, []string) string) ([]OutputFile, error) {
	// read all intermediate kv from input files
	var intermediate []KeyValue
	for _, filename := range inputFiles {
		file, err := os.Open(filename)
		if err != nil {
			return nil, fmt.Errorf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				return nil, fmt.Errorf("cannot decode %v", filename)
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// sort
	sort.Sort(ByKey(intermediate))

	// call Reduce on each distinct key in intermediate[] and print to tmp file
	tmpFileName := fmt.Sprintf("tmp-%v-mr-out-%d", randomString(5), taskNumber)
	tmpFile, _ := os.Create(tmpFileName)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tmpFile.Close()

	// rename output file
	newName := fmt.Sprintf("mr-out-%d", taskNumber)
	if err := os.Rename(tmpFileName, newName); err != nil {
		return nil, fmt.Errorf("cannot rename %v to %v", tmpFileName, newName)
	}

	return []OutputFile{{Partition: taskNumber, FileName: newName}}, nil
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		log.Printf("%v error: %v", rpcname, err)
		return err
	}
	return nil
}
