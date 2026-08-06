package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "os"

// Map functions return a slice of KeyValue.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	args := TaskArgs{
		WorkerId: os.Getpid(),
	}

	reply := TaskReply{}

	for {
		ok := call("Coordinator.RequestTask", &args, &reply)
		if ok {
			switch reply.TaskType {
			case "map":
				doMap(reply, mapf)
			case "reduce":
				doReduce(reply, reducef)
			case "wait":
				time.Sleep(time.Second)
				continue
			}
		} else {
			log.Println("Coordinator unreachable (RequestTask). Shutting down worker.")
			os.Exit(1)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doReduce(reply TaskReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}

	for i := 0; i < reply.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reply.ReduceTaskId)
		file, err := os.Open(filename)

		if err != nil {
			continue
		}

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	tempFile, err := os.CreateTemp("", "mr-out-temp-*")
	if err != nil {
		log.Fatalf("cannot create temp out file")
	}

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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tempFile.Close()
	finalName := fmt.Sprintf("mr-out-%d", reply.ReduceTaskId)
	os.Rename(tempFile.Name(), finalName)

	respArgs := ReportTaskArgs{
		WorkerId: os.Getpid(),
		TaskType: reply.TaskType,
		TaskId:   reply.ReduceTaskId,
	}

	respReply := ReportTaskReply{}
	ok := call("Coordinator.ReportTaskComplete", &respArgs, &respReply)

	if !ok {
		log.Println("Coordinator unreachable (ReportTaskComplete). Shutting down worker.")
		os.Exit(1)
	}
}

func doMap(reply TaskReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}

	content, err := ioutil.ReadAll(file)

	file.Close()
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}

	kva := mapf(reply.Filename, string(content))

	tempFiles := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)

	for i := 0; i < reply.NReduce; i++ {
		tempFile, err := os.CreateTemp("", "mr-temp-*")
		if err != nil {
			log.Fatalf("cannot create temp file")
		}

		tempFiles[i] = tempFile
		encoders[i] = json.NewEncoder(tempFile)
	}

	for _, kv := range kva {
		bucket := ihash(kv.Key) % reply.NReduce
		err := encoders[bucket].Encode(&kv)
		if err != nil {
			log.Fatalf("Cannot encode kv pair")
		}
	}

	for i := 0; i < reply.NReduce; i++ {
		tempFiles[i].Close()
		finalName := fmt.Sprintf("mr-%d-%d", reply.MapTaskId, i)
		os.Rename(tempFiles[i].Name(), finalName)
	}

	reportArgs := ReportTaskArgs{
		WorkerId: os.Getpid(),
		TaskType: reply.TaskType,
		TaskId:   reply.MapTaskId,
	}

	reportReply := ReportTaskReply{}
	ok := call("Coordinator.ReportTaskComplete", &reportArgs, &reportReply)
	if !ok {
		log.Println("Coordinator unreachable (ReportTaskComplete). Shutting down worker.")
		os.Exit(1)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
