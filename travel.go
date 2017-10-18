package main

/*
   	author: Nikolai Baidiukov
   	e-mail: n.baidukov@gmail.com

   	This code is terrible and it is not finished.
	Do not use it at the production stage.
	Use it only for educational purposes.
*/

// TODO: current b-tree is not usable for searching values greater that needed, only works with equality
// TODO: create timeout for response because routines pool may fail more queries
// TODO: in Windows works 5 times slower

// -TODO: replace strings with []byte
// +TODO: cache ToJson
// TODO: recalculate with workers
// TODO: slow tree search
// TODO: use TreeMap for F&M Visits
// TODO: preallocation

import (
	"github.com/valyala/fasthttp"
	"log"
	"os"
	// "runtime/debug"
	"strconv"
	"sync"
	"time"
	//_ "github.com/emirpasic/gods/sets/hashset"
	"fmt"
	//"net/url"
	"runtime"
	_ "runtime/pprof"
	//"net/http"
	"archive/zip"
	"bufio"
	"io"
	"testing/iotest"

	_ "net/http/pprof"
	//"net/http"

	//"github.com/valyala/bytebufferpool"
	"bytes"
	"io/ioutil"
	// "syscall"
)

var (
	VERSION     = "2.2"
	HTTPPort    = "80"
	DataFileDir = "/tmp/data"
	STAGE = 0
)

const (
	DEBUG               = false
	USE_TIMETRACK       = false
	CHECK_LOAD_VALIDITY = true

	USER     = 1
	LOCATION = 2
	VISIT    = 3
)

// Highload server in go: http://marcio.io/2015/07/handling-1-million-requests-per-minute-with-golang/

type RawJson struct {
	fileName string
	rc       io.ReadCloser
	//size     int64
}

func ReadBytes(reader *bufio.Reader, data []byte, delim byte) ([]byte, error) {
	nb := 0
	var err error
	var c byte
	for {
		c, err = reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err == nil {
			//fmt.Printf("%d: %c %s %d %d\n", nb, c, string(data), len(data), cap(data))
			data[nb] = c
			nb++
			if c == delim {
				break
			}
		} else if err != iotest.ErrTimeout {
			panic("Data: " + err.Error())
		}
	}
	return data[:nb], err
	//return string(b[0:nb])
}

func ReadFrom(reader *bufio.Reader, data []byte, delim byte) ([]byte, error) {
	// nStart := int64(len(data))
	nMax := cap(data)
	n := 0 // nStart
	var err error
	var c byte
	if nMax == 0 {
		panic("Array is empty!!!")
		// nMax = 64
		// data = make([]byte, nMax)
	} else {
		data = data[:nMax]
	}
	for {
		if n == nMax {
			nMax *= 2
			bNew := make([]byte, nMax)
			copy(bNew, data)
			data = bNew
		}

		c, err = reader.ReadByte()

		if err == io.EOF {
			return data[:n], err
		}
		if err == nil {
			data[n] = c
			n++
			if c == delim {
				return data[:n], nil
			}
		} else if err != iotest.ErrTimeout {
			panic("Data: " + err.Error())
		}
	}
}

func parseRawJsonArray(wg *sync.WaitGroup, json *RawJson, jsonType int) {
	if USE_TIMETRACK {
		defer TimeTrack(time.Now(), "Json parsing "+json.fileName+": "+strconv.Itoa(jsonType))
	}
	if DEBUG {
		log.Println("-> Json parsing", json.fileName)
	}

	defer wg.Done()

	reader := bufio.NewReader(json.rc)

	var err error
	var str []byte

	var begin int

	//var e IEntity

	// TODO: variable number of spaces
	switch jsonType {
	case USER:
		begin = 11
	case LOCATION:
		begin = 15
	case VISIT:
		begin = 12
	}

	// 2. 3. initial array
	// data := make([]byte, 256) // TODO: use pool

	// experimental pool
	//data := <-PoolGet
	bbuff := GetBuffer()

	for { // all json objects in an array
		// 1. old (25, 33, 25, 26, 26)
		// str, err = reader.ReadSlice('}') // don't use ReadBytes cause it copies memory
		// 2. new (25, 34, 25, 26, 26)
		// str, err = ReadBytes(reader, data, '}') // use: data := make([]byte, 256)
		// 3. experimental with pool
		str, err = ReadFrom(reader, bbuff.B, '}') // use: data := make([]byte, 256)

		if err != nil {
			break
		}

		if len(str) > 2 {
			//log.Println("<" + string(str[begin:]) + ">" + strconv.Itoa(begin))
			// e =
			_, _ = NewEntity(jsonType, str[begin:len(str)-1])
			//e.Print()
		}
		begin = 2
	}
	// TODO: how to clean?

	// PoolPut <- data
	ReleaseBuffer(bbuff)

	reader = nil
	json.rc.Close()
	json.rc = nil
	json = nil
}

func parseFiles(fileChan chan *RawJson) {
	// if USE_TIMETRACK {
	defer TimeTrack(time.Now(), "All Json parsing")
	// }
	var jsonType int

	var wg sync.WaitGroup

	for {
		json, ok := <-fileChan
		if (json.fileName == "") || (ok == false) {
			log.Println("Done parsing!")
			break
		}
		switch json.fileName[0] {
		case 'u':
			jsonType = USER
		case 'l':
			jsonType = LOCATION
		case 'v':
			jsonType = VISIT
		}

		// TODO: remove debug
		//if jsonType == VISIT {
		wg.Add(1)
		// TODO: reader stops earlier than parsing is going to be done
		go parseRawJsonArray(&wg, json, jsonType)
		//}
	}
	log.Println("Waiting parsers...")
	wg.Wait()
}

// func runGC() {
// if USE_TIMETRACK {
// defer TimeTrack(time.Now(), "GC GC GC")
// }

// log.Println("GC GC GC")

// runtime.GC()
// }

func indexData() {
	// if USE_TIMETRACK {
	defer TimeTrack(time.Now(), "All Indexing")
	// }

	log.Println("Indexing")

	// log.Println("Indexing Users")
	// for _, v := range UserIdHash {
	// v.CalcIndexes()
	// }
	// log.Println("Indexing Locations")
	// for _, v := range LocationIdHash {
	// v.CalcIndexes()
	// }
	log.Println("Indexing Visits")
	for _, v := range VisitIdHash {
		v.CalcIndexes()
	}
}

/*

Get requests:
1. GET /<entity>/<id>
Error 404 if no such <id>
Fast response with prepared data.

2. GET: /users/<id>/visits
fromDate < visited_at < toDate
country
toDistance
Filter by Country slice AND Date AND Distance
Sort ASC date

3. GET /locations/<id>/avg
fromDate < visited_at < toDate
fromAge < age using date from options < toAge
gender
Return float with 5 digits after point.
If no places has found then return avg=0.0
Error 400/404 if no such <id>

4. POST /<entity>/<id>
HTTPCode: 200 and Return empty object {}
HTTPCode: 404 if incorrect <id>
HTTPCode: 400 if data in json incorrect (time, age, others)
Id is readonly.

5. POST /<entity>/new
HTTPCode: 200 and Return empty object {}
HTTPCode: 400 if data in json incorrect (time, age, others)


Questions:
- are connections (requests) concurrent? should I use JobWorker pattern?

Stuff:
- place long strings as nosql paradigm says
- cache whole text json for fast response and recalculate only after changes
*/

func prepare() {
	if USE_TIMETRACK {
		defer TimeTrack(time.Now(), "Prepare data")
	}

	testDataDir := DataFileDir

	if len(os.Args) > 2 {
		testDataDir = os.Args[2]
	}

	// epfd, e := syscall.EpollCreate1(0)
	// if e != nil {
	// fmt.Println("epoll_create1: ", e)
	// os.Exit(1)
	// }
	// defer syscall.Close(epfd)

	log.Println("Test data directory:", testDataDir)

	file, err := os.Open(testDataDir + "/options.txt")
	if err != nil {
		log.Fatal(err)
	}
	b, err := ioutil.ReadAll(file)

	options := bytes.Split(b, []byte{'\n'})
	currentTime, _ := strconv.ParseInt(string(options[0]), 10, 64)
	CURRENT_TIME = time.Unix(currentTime, 0)

	log.Println("CURRENT_TIME:", CURRENT_TIME.Unix())

	file.Close()

	zipFileName := testDataDir + "/data.zip"
	var unzipChan chan *RawJson = make(chan *RawJson)
	log.Println("Unzipping:", zipFileName)
	// unpack all files into memory

	zipReader, err := zip.OpenReader(zipFileName)
	if err != nil {
		close(unzipChan)
		log.Panicln(err)
	}

	go Unzip(zipReader, unzipChan)
	parseFiles(unzipChan)
	zipReader.Close()

	// go UpdateIndexes()
	// go DispatchIndexChannel()

	indexData()
}

func MonitorRuntime2() {
	for {
		select {
		case <-time.After(time.Second):
			fmt.Println("\tgoroutines:", runtime.NumGoroutine())
		}
	}
}

func main() {
	log.Println("Version:", VERSION)
	// go MonitorRuntime()
	//go MonitorRuntime2()
	//handler.PathPrefix("/debug/pprof/heap").HandlerFunc(pprof.Heap)

	//go func() {
	//	log.Println(http.ListenAndServe("localhost:6060", nil))
	//}()

	// TUNING
	// runtime.GOMAXPROCS(4)
	// debug.SetGCPercent(90)
	// TUNING

	port := HTTPPort
	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	// PoolGet, PoolPut = MakeRecycler(4096, 30, 1000)
	// VisitCalcIndexChannel = make(chan IEntity)
	// VisitPostUpdateChannel = make(chan IEntity)

	// IndexChannel = make(chan IEntity)

	// allocate memory pool
	var buf [512]*fasthttp.ByteBuffer

	for i := 0; i < 512; i++ {
		buf[i] = GetBuffer()
	}
	for i := 0; i < 512; i++ {
		ReleaseBuffer(buf[i])
	}

	STAGE = 0
	prepare()
	MemStat()
	STAGE = 1

	//debug.FreeOSMemory()

	// time.Sleep(time.Duration(5) * time.Second)
	// memStat()

	//log.Println("Users len:", UserIdHash.Size())
	//log.Println("Locations len:", LocationIdHash.Size())
	//log.Println("Visits len:", VisitIdHash.Size())

	log.Println("Users len:", len(UserIdHash))
	log.Println("Locations len:", len(LocationIdHash))
	log.Println("Visits len:", len(VisitIdHash))

	//usr := UserIdHash["229"]
	//usr.Print()
	//
	//loc := LocationIdHash["131"]
	//loc.Print()
	//
	//visit := VisitIdHash["10580"]
	//visit.Print()

	//for i := range UserIdHash {
	//	u := UserIdHash[i]
	//	if u.VisitsCount >= 10 {
	//		fmt.Println("UserId:", u.Id, "count:", u.VisitsCount, u.VisitsByDate.Keys())
	//	}
	//}

	/*testUser := UserIdHash["84"]

	fmt.Println(testUser.VisitsByDate.Get(int64(949644053)))
	fmt.Println(testUser.VisitsByDate.Keys())
	//fmt.Println(testUser.VisitsByDate.Values())

	it := testUser.VisitsByDate.Iterator()
	for it.Next() {
		key := it.Key().(int64)
		v := it.Value().(*Visit)
		fmt.Println(key, time.Unix(key, 0), v.LocationPtr.Country, v.LocationPtr.Distance)
	}
	fmt.Println("-----------")
	arr := testUser.GetUserVisits(int64(1298211640), int64(1355930612), "", 100)
	vit := arr.Iterator()
	for vit.Next() {
		v := vit.Value().(*Visit)
		fmt.Println(time.Unix(v.VisitedAtInt, 0), v.LocationPtr.Country, v.LocationPtr.Distance)
	}*/

	//countries := hashset.New()
	//for i := range LocationIdHash {
	//	l := LocationIdHash[i]
	//	countries.Add(string(l.Country))
	//}
	//fmt.Println(countries)

	log.Println("Starting on port:", port)

	var concurrency int
	// if DEBUG {
	concurrency = 1024 * 256
	// } else {
	// concurrency = 10000
	// }

	s := &fasthttp.Server{
		Handler:     fasthttp.TimeoutHandler(RequestHandler, time.Duration(2)*time.Second, "2 seconds timeout!!!"),
		Concurrency: concurrency,
		Name: "X",
		// Other Server settings may be set here.
	}
	// FIX: 4
	if err := s.ListenAndServe(":" + port); err != nil {
		log.Fatalf("error in ListenAndServe: %s", err)
	}

	log.Println("Routing done!")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
