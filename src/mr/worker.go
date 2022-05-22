package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type WorkerClient struct {
	workerId   string
	TaskId     string
	TaskType   TaskType
	TaskStatus TaskStatus
	InputFileUrl    string
	OutPutFileLimit int
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	client := WorkerClient{
		workerId:        "", //uuid
		TaskId:          "",
		TaskType:        "",
		TaskStatus:      "",
		OutPutFileLimit: 0,
		mapf: mapf,
		reducef: reducef,
	}
	client.enableBackReport()
	for {
		task, err := CallGetTask()
		if err != nil {
			continue
		}
		if task.Finished {
			return
		}
		if !task.Succeed {
			continue
		}
		client.receiveNewTask(task)
		client.handleTask()
	}
}

func (c WorkerClient) receiveNewTask(response GetTaskResponse)  {
	c.TaskId = response.TaskId
	c.TaskStatus = TaskStatus_RUNNING
	c.TaskType = response.TaskType
	c.InputFileUrl = response.InputFileUrl
	c.OutPutFileLimit = response.OutPutFileLimit
}

func (c WorkerClient) enableBackReport()  {
	// 每几秒钟上报一次
	// 注意与任务执行完成一起加锁
}

func (c WorkerClient) reportTaskFinished()  {
	// 注意与BackReport一起加锁
}

func (c WorkerClient) outputMapResult(kva []KeyValue) error {
	sort.Sort(ByKey(kva))
	outputFiles := map[string]*os.File
	i := 0
	for i < len(kva) {
		key := kva[i].Key
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		outputFileName := strconv.Itoa(ihash(key) % c.OutPutFileLimit)
		file, ok := outputFiles[outputFileName]
		if !ok {
			file, _ = os.CreateTemp(outputFileName, c.workerId)
			outputFiles[outputFileName] = file
		}
		for k := i; k < j; k++ {
			_, err := fmt.Fprintln(file, kva[k].Value)
			if err != nil {
				return err
			}
		}
		i = j
	}
	return nil
}

func (c WorkerClient) handleTask()  {
	switch c.TaskType {
	case TaskType_MAP:
		c.handleMap()
	case TaskType_REDUCE:
		c.handleReduce()
	}
	c.reportTaskFinished()
}

func (c WorkerClient) handleMap() {
	file, err := os.Open(c.InputFileUrl)
	if err != nil {
		log.Fatalf("cannot open %v", c.InputFileUrl)
	}
	content, err := ioutil.ReadAll(file)
	kva := c.mapf(c.InputFileUrl, string(content))
	c.outputMapResult(kva)
	c.reportTaskFinished()
}
func (c WorkerClient) handleReduce() {
	entries, _ := os.ReadDir(c.TaskId)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		bytes, _ := os.ReadFile(entry.Name())
		strings.Split() 
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallGetTask() (GetTaskResponse, error) {
	// declare an argument structure.
	args := GetTaskRequest{
		WorkerId: strconv.Itoa(os.Getuid()),
	}
	// declare a reply structure.
	response := GetTaskResponse{}
	// send the RPC request, wait for the reply.
	if err := call("Coordinator.GetTask", &args, &response); err != nil {
		return response, err
	}
	return response, nil
}

func CallReportTaskStatus(taskStatus TaskStatus, taskType TaskType, outputFileName string) (ReportTaskStatusResponse, error) {
	// declare an argument structure.
	args := ReportTaskStatusRequest{
		TaskStatus:     taskStatus,
		TaskType:       taskType,
		OutputFileName: outputFileName,
	}
	// declare a reply structure.
	response := ReportTaskStatusResponse{}
	// send the RPC request, wait for the reply.
	if err := call("Coordinator.ReportTaskStatus", &args, &response); err != nil {
		return response, err
	}
	return response, nil
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
	return err
}
