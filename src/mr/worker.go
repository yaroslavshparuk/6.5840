package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

func mapTask(filename string, taskNumber int, nReduce int, mapf func(string, string) []KeyValue) {
	type ByKey []KeyValue
	intermediate := make([][]KeyValue, nReduce)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		intermediate[r] = append(intermediate[r], kv)
	}
	args := MapDoneArgs{FileName: filename}
	for r, kva := range intermediate {
		oname := fmt.Sprintf("mr-%d-%d", taskNumber, r)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			enc.Encode(&kv)
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
		args.MappedFiles = append(args.MappedFiles, oname)
	}
	CallMapDone(args)
}

func reduceTask(filename string, intermediateFiles []string, taskNumber int, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for m := 0; m < len(intermediateFiles); m++ {
		file, err := os.Open(intermediateFiles[m])
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFiles[m])
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

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	oname := fmt.Sprintf("mr-out-%d", taskNumber)
	ofile, _ := os.Create(oname)

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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	finalErr := ofile.Close()
	if finalErr != nil {
		fmt.Println(finalErr)
	}
	CallReduceDone(ReduceDoneArgs{FileName: filename})
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string, uuid string) bool {
  fmt.Println("Requesting task... " + uuid + "\n")
	task, err := CallRequestTask()
	if err != nil {
		return false
	}
	if task.IsMapTask {
    fmt.Println("Map task returned for " + uuid + "\n")
		mapTask(task.FileName, task.TaskNumber, task.ReduceNumber, mapf)
    fmt.Println("Map task done by " + uuid)
		return false
	}
	if task.IsReduceTask {
    fmt.Println("Reduce task returned for " + uuid + "\n")
		reduceTask(task.FileName, task.MappedFiles, task.TaskNumber, reducef)
    fmt.Println("Reduce task done " + uuid)
		return false
	}
	return CallJobDone()
}

func CallMapDone(args MapDoneArgs) error {
	ok := call("Coordinator.MapDone", &args, &Args{})
	if ok {
		return nil
	} else {
		fmt.Printf("call failed!\n")
		return errors.New("map done Task RPC call failed")
	}
}

func CallReduceDone(args ReduceDoneArgs) error {
	ok := call("Coordinator.ReduceDone", &args, &Args{})
	if ok {
		return nil
	} else {
		fmt.Printf("call failed!\n")
		return errors.New("map done Task RPC call failed")
	}
}

func CallJobDone() bool {
	reply := JobDoneReply{}
	ok := call("Coordinator.JobDone", &Args{}, &reply)
	if ok {
		return reply.done
	} else {
		return false
	}
}

func CallRequestTask() (TaskRequestReply, error) {
	reply := TaskRequestReply{}
	ok := call("Coordinator.RequestTask", &Args{}, &reply)
	if ok {
		return reply, nil
	} else {
		return reply, errors.New("request Task RPC call failed")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
    return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
  os.Exit(1)
	return false
}
