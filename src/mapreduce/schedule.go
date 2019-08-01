package mapreduce

import (
	"fmt"
	"time"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

func RunTask(w string, args *DoTaskArgs, registerChan chan string, p chan int) {
	ok := call(w, "Worker.DoTask", args, new(struct{}))

	go func() { registerChan <-w } ()
	if (ok == false) {
		p <-args.TaskNumber;
	  //fmt.Printf("schedule error");
	} 
/*	else {
	  *num = *num + 1;
	  fmt.Print(*num)
	}*/ 
  //registerChan <-w	  
}

/*func InputTask(p chan int, n int) {
	for i := 0; i < n; i ++ {
		p <-i;
	}
}*/



func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	var n int
	var w string
	nMap := len(mapFiles)
	if phase == mapPhase {
	  n = nMap
	} else {
	  n = nReduce
	}
	p := make(chan int, n)
	for i := 0; i < n; i ++ {
		p <-i;
	}
	//q := make(chan int, n)
	//go InputTask(p, n) 
	for ; ; {
    timeout := make(chan bool, 1)
    go func() {
        time.Sleep(1e9)    //等待1秒钟
        timeout <- true
    }()
    i := -1;
    // 然后，我们把timeout这个channel利用起来
    select {
        case i = <-p:
            // 从ch中读到数据
        case <- timeout: { break; }
            // 一直没有从ch中读取到数据，但从timeout中读取到数据
    }
		//i := <-p;
		if i < 0 { break }
    w = <-registerChan
 	  args := new(DoTaskArgs)
	  args.JobName = jobName
	  args.Phase = phase	    
	  args.TaskNumber = i
	  args.NumOtherPhase = nReduce + nMap - n
	  if (phase == mapPhase) { args.File = mapFiles[i] }
	  go RunTask(w, args, registerChan, p);
	}
  time.Sleep(1e9)
	for i := 0; i < 2; i ++ {
	  <-registerChan
	}
  //close(p);
  //close(registerChan);
	   
	fmt.Printf("Schedule: %v done\n", phase)
}
