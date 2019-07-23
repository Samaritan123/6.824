package mapreduce

import (
  "os"
  "sort"
  "log"
  "strconv"
  "encoding/json"
)

type ByKey []KeyValue

func (a ByKey) Len() int { return len(a) }
func (a ByKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { 
	p, _ := strconv.Atoi(a[i].Key)
	q, _ := strconv.Atoi(a[j].Key)
	return p < q
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	var err error
	var kv KeyValue;
	Slice := make([]KeyValue, 0);
	for i := 0; i < nMap; i ++ {
	  p, err := os.Open(reduceName(jobName, i, reduceTask))
    if err != nil {
        log.Fatal(err)
    }
	  f := json.NewDecoder(p)
	  for ;; {
	    err := f.Decode(&kv)
	    if err != nil { break }
	    Slice = append(Slice, kv)
	  }
	  p.Close()
	}
	p, err := os.Create(mergeName(jobName, reduceTask))
  if err != nil {
    log.Fatal(err)
  }
	enc := json.NewEncoder(p)
	sort.Sort(ByKey(Slice))
	for i := 0; i < len(Slice); i ++ {
	  if i == 0 {
	    enc.Encode(KeyValue{Slice[i].Key, reduceF(Slice[i].Key, []string{""})})
	    continue
	  }
	  if Slice[i].Key == Slice[i - 1].Key { continue }
      enc.Encode(KeyValue{Slice[i].Key, reduceF(Slice[i].Key, []string{""})})	
    }
  p.Close()
}
