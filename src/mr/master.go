package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	//for resp type
	JOB_TYPE_MAP    = 1
	JOB_TYPE_REDUCE = 2
	JOB_TYPE_STUB   = 3
	JOB_TYPE_EXIT   = -1000
	// for req type
	JOB_STATUS_FREE   = 0
	JOB_STATUS_MAP    = 1
	JOB_STATUS_REDUCE = 2
	JOB_STATUS_ERROR  = -999
	TIME_LIMIT_SEC    = 10
)

type Master struct {
	// Your definitions here.
	files          []string
	TodoMapTask    []int
	TodoReduceTask []int
	MapTask        map[int]*TaskInfo
	ReduceTask     map[int]*TaskInfo
	NMap           int
	NReduce        int
	MapLock        sync.Mutex
	ReduceLock     sync.Mutex
	DoneMapTask    []int //index of input file in the slice
	DoneReduceTask []int //num of Reduce task
	ExitChan       chan int
}

type TaskInfo struct {
	Pid       int
	StartTime time.Time
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) Echo(req *VRequest, resp *VResponse) error {
	status := req.JobStatus
	pid := req.Pid
	if m.getDoneReduceTaskNum() == m.NReduce { //Job finished
		resp.JobType = JOB_TYPE_EXIT
		m.ExitChan <- 1
		return nil
	}
	if status == -999 { //task error
		if info, ok := m.MapTask[req.TaskNum]; ok && info.Pid == req.Pid {
			//Redo the task
			m.MapLock.Lock()
			m.TodoMapTask = append(m.TodoMapTask, req.TaskNum)
			delete(m.MapTask, req.TaskNum)
			m.MapLock.Unlock()
		}
		status = 0 //set back to free worker
	}

	if status == JOB_STATUS_MAP { //get res of Map task
		log.Printf("Pid: %v, Map task: %v", pid, req.TaskNum)
		m.MapLock.Lock()
		m.DoneMapTask = append(m.DoneMapTask, req.TaskNum)
		delete(m.MapTask, req.TaskNum)
		m.MapLock.Unlock()
	} else if status == JOB_STATUS_REDUCE { //get res of Reduce task
		m.ReduceLock.Lock()
		m.DoneReduceTask = append(m.DoneReduceTask, req.TaskNum)
		delete(m.ReduceTask, req.TaskNum)
		m.ReduceLock.Unlock()
	}

	// assign new task
	if m.getDoneMapTaskNum() < m.NMap {
		//Map
		taskNum := m.getTodoMapTask()
		if taskNum != -1 {
			m.prepareMapTask(taskNum, pid, resp)
			log.Printf("Assign Map task %v to Pid %v", taskNum, pid)
			return nil
		}
	} else if m.getDoneReduceTaskNum() < m.NReduce {
		//Reduce
		resp.JobType = 2
		taskNum := m.getTodoReduceTask()
		if taskNum != -1 {
			m.prepareReduceTask(taskNum, pid, resp)
			log.Printf("Assign Reduce task %v to Pid %v", taskNum, pid)
			return nil
		}
	}
	resp.JobType = JOB_STATUS_STUB
	log.Print("Assign nothing")
	return nil
}

func (m *Master) getDoneMapTaskNum() int {
	m.MapLock.Lock()
	defer m.MapLock.Unlock()
	return len(m.DoneMapTask)
}

func (m *Master) getDoneReduceTaskNum() int {
	m.ReduceLock.Lock()
	defer m.ReduceLock.Unlock()
	return len(m.DoneReduceTask)
}

func (m *Master) getTodoMapTask() int {
	if len(m.TodoMapTask) == 0 {
		return -1
	}
	m.MapLock.Lock()
	defer m.MapLock.Unlock()
	if len(m.TodoMapTask) == 0 {
		return -1
	}
	res := m.TodoMapTask[0]
	m.TodoMapTask = m.TodoMapTask[1:]
	return res
}

func (m *Master) getTodoReduceTask() int {
	if len(m.TodoReduceTask) == 0 {
		return -1
	}
	m.ReduceLock.Lock()
	defer m.ReduceLock.Unlock()
	if len(m.TodoReduceTask) == 0 {
		return -1
	}
	res := m.TodoReduceTask[0]
	m.TodoReduceTask = m.TodoReduceTask[1:]
	return res
}

func (m *Master) prepareMapTask(taskNum, pid int, resp *VResponse) {
	resp.JobType = JOB_TYPE_MAP
	resp.InputFile = m.files[taskNum]
	resp.TaskNum = taskNum
	resp.NMap = m.NMap
	resp.NReduce = m.NReduce
	resp.TimeLimitSec = TIME_LIMIT_SEC

	m.MapLock.Lock()
	defer m.MapLock.Unlock()
	m.MapTask[taskNum] = &TaskInfo{
		Pid:       pid,
		StartTime: time.Now(),
	}
}

func (m *Master) prepareReduceTask(taskNum, pid int, resp *VResponse) {
	resp.JobType = JOB_TYPE_REDUCE
	resp.NMap = m.NMap
	resp.NReduce = m.NReduce
	resp.InputFile = ""
	resp.TaskNum = taskNum

	m.ReduceLock.Lock()
	defer m.ReduceLock.Unlock()
	m.ReduceTask[taskNum] = &TaskInfo{
		Pid:       pid,
		StartTime: time.Now(),
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	time.Sleep(time.Second * 3)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//

func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if len(m.DoneReduceTask) == m.NReduce {
		ret = true
	}

	return ret
}

func (m *Master) countTime() {
	for {
		select {
		case _ = <-m.ExitChan:
			log.Print("Over, count time thread quit!")
			break
		default:
			log.Print("Count time thread active...")
			time.Sleep(time.Second)
		}
		for taskNum, taskInfo := range m.MapTask {
			if time.Since(taskInfo.StartTime).Seconds() > float64(TIME_LIMIT_SEC) {
				log.Printf("Map Task %v at Pid %v timeout.", taskNum, taskInfo.Pid)
				m.MapLock.Lock()
				m.TodoMapTask = append(m.TodoMapTask, taskNum)
				delete(m.MapTask, taskNum)
				m.MapLock.Unlock()
			}
		}

		for taskNum, taskInfo := range m.ReduceTask {
			if time.Since(taskInfo.StartTime).Seconds() > float64(TIME_LIMIT_SEC) {
				log.Printf("Reduce Task %v at Pid %v timeout.", taskNum, taskInfo.Pid)
				m.ReduceLock.Lock()
				m.TodoReduceTask = append(m.TodoReduceTask, taskNum)
				delete(m.ReduceTask, taskNum)
				m.ReduceLock.Unlock()
			}
		}
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		NMap:           len(files),
		NReduce:        nReduce,
		files:          files,
		MapTask:        make(map[int]*TaskInfo),
		ReduceTask:     make(map[int]*TaskInfo),
		DoneMapTask:    []int{},
		DoneReduceTask: []int{},
		ExitChan:       make(chan int),
	}
	// Your code here.
	for idx, _ := range files {
		m.TodoMapTask = append(m.TodoMapTask, idx)
	}
	for idx := 0; idx < nReduce; idx++ {
		m.TodoReduceTask = append(m.TodoReduceTask, idx)
	}
	go m.countTime()

	m.server()
	return &m
}
