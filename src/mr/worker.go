package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

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

type WorkerStatus struct {
	code    int
	TaskNum int
	Pid     int
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

// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	ws := &WorkerStatus{
		code:    0,
		TaskNum: -1,
		Pid:     os.Getpid(),
	}
	exit := false
	for !exit {
		//get tasks or send result
		resp, err := Echo(ws)

		if err != nil {
			log.Fatalf("master network error!")
			continue
		}

		switch resp.JobType {
		case JOB_TYPE_MAP: //Map tasks
			time.Sleep(time.Second)
			err := MapHandler(ws, &resp, mapf)
			if err != nil {
				log.Printf("Map handler error: %s", err)
				ws.code = JOB_STATUS_ERROR
			} else {
				ws.code = JOB_STATUS_MAP
			}
			continue
		case JOB_TYPE_REDUCE: //Reduce tasks
			//read intermediate data from disk
			err := ReduceHandler(ws, &resp, reducef)
			if err != nil {
				log.Printf("Reduce handler error: %s", err)
				ws.code = JOB_STATUS_ERROR
			} else {
				ws.code = JOB_STATUS_REDUCE
			}
			time.Sleep(time.Second)
			continue
		case JOB_TYPE_EXIT:
			log.Printf("Job finished. Exit!")
			exit = true
			break
		default:
			ws.code = JOB_STATUS_FREE
			log.Print("Do nothing")
			time.Sleep(time.Second)
			continue
		}
	}
}

func ReduceHandler(ws *WorkerStatus, resp *VResponse, reducef func(string, []string) string) error {
	ws.TaskNum = resp.TaskNum
	kva := []KeyValue{}
	var err error
	for idx := 0; idx < resp.NMap; idx++ {
		filename := fmt.Sprintf("mr-%v-%v", idx, ws.TaskNum)
		var file *os.File
		file, err = os.Open(filename)
		if err != nil {
			log.Printf("Open intermediate file: %s, error: %s", filename, err)
			file.Close()
			return err
		}
		dec := json.NewDecoder(file)
		ptr := 0
		for {
			var kv KeyValue
			ptr++
			if err = dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	outputFileName := fmt.Sprintf("mr-%v", ws.TaskNum)
	ofile, _ := os.Create(outputFileName)
	defer ofile.Close()
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	return nil
}

func MapHandler(ws *WorkerStatus, resp *VResponse, mapf func(string, string) []KeyValue) error {
	ws.TaskNum = resp.TaskNum
	filename := resp.InputFile
	file, err := os.Open(resp.InputFile)
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %s\n", filename)
		return err
	}
	file.Close()
	kva := mapf(filename, string(content))
	//output Intermediate
	NReduce := resp.NReduce
	intermediate := make([][]KeyValue, NReduce)
	for i := 0; i < NReduce; i++ {
		intermediate[i] = []KeyValue{}
	}
	//group by
	for _, kv := range kva {
		idx := ihash(kv.Key) % NReduce
		intermediate[idx] = append(intermediate[idx], kv)
	}
	//write intermediate data to disk
	tmpfiles := []string{}
	for i := 0; i < NReduce; i++ {
		interFileName := fmt.Sprintf("mr-%v-%v", ws.TaskNum, i)
		if _, err := os.Stat(interFileName); os.IsExist(err) {
			os.Remove(interFileName)
		}
		tmpf, err := ioutil.TempFile("", interFileName)
		defer tmpf.Close()
		if err != nil {
			log.Fatalf("Create tmp file error: %s", err)
			continue
		}
		tmpfiles = append(tmpfiles, tmpf.Name())

		//ofile, _ := os.Create(interFileName)
		if err != nil {
			log.Fatalf("Open intermediate file error: %s", err)
			return err
		}
		enc := json.NewEncoder(tmpf)
		for _, kv := range intermediate[i] {
			err = enc.Encode(&kv)
			if err != nil {
				return err
			}
		}
		if err != nil {
			log.Fatalf("encode error: %s", err)
			return err
		}
		//ofile.Close()
	}
	ptr := 0
	for _, tmpf := range tmpfiles {
		interFileName := fmt.Sprintf("./mr-%v-%v", ws.TaskNum, ptr)
		log.Printf("Rename %s to %s", tmpf, interFileName)
		os.Rename(tmpf, interFileName)
		ptr++
	}
	return nil
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

func Echo(ws *WorkerStatus) (VResponse, error) {
	args := VRequest{
		JobStatus: ws.code,
		TaskNum:   ws.TaskNum,
		Pid:       ws.Pid,
	}
	resp := VResponse{}
	call("Master.Echo", &args, &resp)
	return resp, nil
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
