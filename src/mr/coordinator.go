package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type FileTask struct {
	fileName      string
	mapStarted    bool
	mapDone       bool
	mappedFiles   []string
}

type Coordinator struct {
	fileTasks      []FileTask
	filesToProcess []string
	reduceStarted bool
  reduceDone bool
	nReduce        int
	mutex          sync.Mutex
}

func (c *Coordinator) RequestTask(args *Args, reply *TaskRequestReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for num, fileName := range c.filesToProcess {
		fileTask, found := c.getFileTask(fileName)
		if !found && !fileTask.mapStarted {
			newTask := FileTask{
				fileName:   fileName,
				mapStarted: true,
				mapDone:    false,
			}
			reply.FileName = fileName
			reply.TaskNumber = num
			reply.ReduceNumber = c.nReduce
			reply.IsMapTask = true
			c.fileTasks = append(c.fileTasks, newTask)
			fmt.Printf("Gave %v to map\n", fileName)
			return nil
    }
  }
  if(!c.reduceStarted) {
   for _, ft := range c.fileTasks{
      if(!ft.mapDone){
        return nil;
      }
for _, mf := range ft.mappedFiles{
				reply.MappedFiles = append(reply.MappedFiles, mf)
    }
  }

			reply.IsReduceTask = true
      c.reduceStarted  = true
			fmt.Printf("Reduce started\n")
  }
  			return nil
}

func (c *Coordinator) MapDone(args *MapDoneArgs, reply *EmptyReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	fileTask, found := c.getFileTask(args.FileName)
	if found {
		for _, mapFile := range args.MappedFiles {
			fileTask.mappedFiles = append(fileTask.mappedFiles, mapFile)
		}
		fileTask.mapDone = true
	} else {
		return errors.New("No such task started")
	}
	return nil
}

func (c *Coordinator) ReduceDone(args *ReduceDoneArgs, reply *EmptyReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
  c.reduceDone = true
	return nil
}
func (c *Coordinator) getFileTask(fileName string) (*FileTask, bool) {
	for i, task := range c.fileTasks {
		if task.fileName == fileName {
			return &c.fileTasks[i], true
		}
	}
	return &FileTask{}, false
}

// start a thread that listens for RPCs from worker.go
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

func (c *Coordinator) JobDone(args *Args, reply *JobDoneReply) error {
	reply.done = c.Done()
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if len(c.fileTasks) == 0 {
		return false
	}
	
	return c.reduceDone; 
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println(files)
	c := Coordinator{filesToProcess: files, nReduce: nReduce}

	// Your code here.

	c.server()
	return &c
}
