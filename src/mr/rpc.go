package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType string

const (
	TaskType_MAP    TaskType = "map"
	TaskType_REDUCE TaskType = "reduce"
)

type TaskStatus string

const (
	TaskStatus_WAITING TaskStatus = "waiting"
	TaskStatus_RUNNING TaskStatus = "running"
	TaskStatus_SUCCESS TaskStatus = "success"
)

// GetTaskRequest 获取任务请求
type GetTaskRequest struct {
	WorkerId string
}

// GetTaskResponse 获取任务响应
type GetTaskResponse struct {
	Succeed         bool
	Finished        bool
	TaskId          string
	TaskType        TaskType
	InputFileUrl    string
	OutPutFileLimit int
}

// ReportTaskStatusRequest 上报任务执行状态请求
type ReportTaskStatusRequest struct {
	WorkerId       string
	TaskId         string
	TaskType       TaskType
	TaskStatus     TaskStatus
	OutputFileName string
}

// ReportTaskStatusResponse 上报任务执行状态响应
type ReportTaskStatusResponse struct {
	Msg string
}

func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
