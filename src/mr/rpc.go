package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	TaskType     string // map or reduce
	Filename     string
	MapTaskId    int
	ReduceTaskId int
	NReduce      int
	NMap         int
}

type ReportTaskArgs struct {
	WorkerId int
	TaskType string
	TaskId   int
}

type ReportTaskReply struct {

}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
