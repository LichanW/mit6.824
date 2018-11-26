package mapreduce

import (
	"os"
	"hash/fnv"
	"log"
	"encoding/json"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	// open input file
	inputFile, err := os.Open(inFile)
	if err != nil {
		log.Fatal("common map, open input file ", inFile, "error:", err)	
	}

	// close input file before function exit
	defer inputFile.Close()

	// get input files status, which incluse file size
	fileInfo, err := inputFile.Stat()
	if err != nil {
		log.Fatal("common map, get inputFile stat", inFile, "error:", err)
	}

	// read files contents, trans to a string
	data := make([]byte, fileInfo.Size())
	_, err = inputFile.Read(data)
	if err != nil {
		log.Fatal("doMap: read input file ", inFile, " error: ", err)
	}

	// use mapF to make keyValues pairs
	keyValues := mapF(inFile, string(data))
	// use ihash function to map keyValue pairs to intermediate files
	
	// make a list of reduce file
	// reduce file should be located at GFS(google file system)
	// but for simple test, reduce file stored at local file system.
	var reducesEncoder []*json.Encoder
	for i:=0 ; i < nReduce; i++ {
		fileName := reduceName(jobName, mapTaskNumber, i)
		// reducesFiles = append(reducesFiles, fileName)
		reduceFile, err := os.Create(fileName)
		if err != nil {
			log.Fatal("make reduce failes", fileName, "error", err)
		}

		defer reduceFile.Close()
		reducesEncoder = append(reducesEncoder, json.NewEncoder(reduceFile))
	}

	// map keyvalues to nReduces files
	for _, kv := range keyValues {
		reduceInter := ihash(kv.Key) % uint32(nReduce)
		reduces := reducesEncoder[reduceInter]
		err := reduces.Encode(&kv)
		if err != nil {
			log.Fatal("do map encode error", err)
		}
	} 
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
