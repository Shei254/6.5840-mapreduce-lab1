package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	PhaseMap    = 0
	PhaseReduce = 1
	PhaseDone   = 2

	STATUS_IDLE       = 0
	STATUS_INPROGRESS = 1
	STATUS_COMPLETE   = 2
)

type Coordinator struct {
	mu    sync.Mutex
	files []string
	phase int

	mapStatuses []int
	mapTimes    []time.Time

	mapsDone int

	reduceStatuses []int
	reduceTimes    []time.Time

	reduceDone int
	nReduce    int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == PhaseMap {
		for i, status := range c.mapStatuses {
			if status == STATUS_IDLE {
				c.mapStatuses[i] = STATUS_INPROGRESS
				c.mapTimes[i] = time.Now()
				reply.TaskType = "map"
				reply.Filename = c.files[i]
				reply.MapTaskId = i
				reply.NReduce = c.nReduce
				return nil
			} else if status == STATUS_INPROGRESS {
				if time.Since(c.mapTimes[i]) > 10*time.Second {
					c.mapStatuses[i] = STATUS_IDLE
					//return nil
				}
			}
		}
		reply.TaskType = "wait"
	} else if c.phase == PhaseReduce {
		for i, status := range c.reduceStatuses {
			if status == STATUS_IDLE {
				c.reduceStatuses[i] = STATUS_INPROGRESS
				c.reduceTimes[i] = time.Now()
				reply.TaskType = "reduce"
				reply.ReduceTaskId = i
				reply.NMap = len(c.files)
				return nil
			} else if status == STATUS_INPROGRESS {
				if time.Since(c.reduceTimes[i]) > 10*time.Second {
					c.reduceStatuses[i] = STATUS_IDLE
				}
			}
		}
		reply.TaskType = "wait"
	} else {
		reply.TaskType = "wait"
	}

	return nil
}

func (c *Coordinator) ReportTaskComplete(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case "map":
		if c.mapStatuses[args.TaskId] != STATUS_COMPLETE {
			c.mapStatuses[args.TaskId] = STATUS_COMPLETE
			c.mapsDone++

			if c.mapsDone == len(c.files) {
				c.phase = PhaseReduce
				log.Println("All Map tasks completed. Transitioning to Reduce phase.")
			}
		}
	case "reduce":
		if c.reduceStatuses[args.TaskId] != STATUS_COMPLETE {
			c.reduceStatuses[args.TaskId] = STATUS_COMPLETE
			c.reduceDone++

			if c.reduceDone == c.nReduce {
				c.phase = PhaseDone
				log.Println("All Reduce tasks completed. Job finished.")
			}
		}
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	ret := c.phase == PhaseDone

	// Your code here.
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mu:             sync.Mutex{},
		nReduce:        nReduce,
		files:          files,
		mapStatuses:    make([]int, len(files)), // Size = number of input files
		mapTimes:       make([]time.Time, len(files)),
		reduceStatuses: make([]int, nReduce),
		reduceTimes:    make([]time.Time, nReduce),
		phase:          PhaseMap,
	}

	// Your code here.

	c.server(sockname)
	return &c
}
