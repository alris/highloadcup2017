package main

import (
	"bytes"
	"github.com/valyala/fasthttp"
	"log"
	"net/url"
	"strconv"
	"sync"
	"time"
)

var (
	VISIT_ARRAY     = []byte("{\"visits\": [")
	VISIT_ARRAY_END = []byte("]}")
	MARK_FIELD      = []byte("\"mark\": ")
	VISIT_FIELD     = []byte("\"visited_at\": ")
	PLACE_FIELD     = []byte("\"place\": ")

	LOC_AVG     = []byte("{\"avg\": ")
	LOC_AVG_END = []byte("}")

	CURRENT_TIME = time.Unix(1503695491, 0) // default for debug
)

const (
	VIS_FROM_DATE = "fromDate"
	VIS_TO_DATE   = "toDate"
	VIS_COUNTRY   = "country"
	VIS_DISTANCE  = "toDistance"

	LOC_FROM_DATE = "fromDate"
	LOC_TO_DATE   = "toDate"
	LOC_FROM_AGE  = "fromAge"
	LOC_TO_AGE    = "toAge"
	LOC_GENDER    = "gender"
)

func logRequest(ctx *fasthttp.RequestCtx) {
	if DEBUG {
		if ctx.IsPost() {
			log.Println("POST", string(ctx.RequestURI()), string(ctx.PostBody()))
		} else {
			log.Println("GET", string(ctx.RequestURI()))
		}
	}
}

func requestUser(id int64, ctx *fasthttp.RequestCtx, path [][]byte) error {
	//if !IsNum(path[2]) {
	//	ctx.SetStatusCode(fasthttp.StatusBadRequest)
	//	return nil
	//}

	// TODO: cache string(path[2]) in RequestHandler
	// userId, _ := strconv.ParseUint(string(path[2]), 10, 64)
	userId, ok := strToInt64(path[2])
	if !ok {
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		return nil
	}
	//u := UserIdHash[userId]
	u := FindUser(userId, false)
	if u == nil {
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		return nil
	}

	// TODO: why len(path) increased by 1?

	// /users/<id>
	if len(path) == 3 {
		if DEBUG {
			log.Println("@ user request:", string(path[2]))
		}
		ctx.SetStatusCode(fasthttp.StatusOK)

		//buf := <- PoolGet
		bbuff := GetBuffer()

		buf := u.ToJson(bbuff.B)
		ctx.SetBody(buf)

		// PoolPut <- buf
		ReleaseBuffer(bbuff)

		return nil
	}

	// /users/<id>/visits
	if len(path) == 4 && path[3] != nil {
		pathLen := len(path[3]) // visits?params...

		if pathLen >= 6 && path[3][0] == 'v' && path[3][4] == 't' {
			if DEBUG {
				log.Println("@ user visits:", id)
			}

			var strParams []byte
			var fromDate int64 = MIN_TIMESTAMP
			toDate := time.Now().Unix() // TODO: use from options
			country := ""
			var toDistance int64 = 100000000
			var ok bool

			var prop []byte
			var value []byte
			// var err error

			if pathLen > 6 && path[3][6] == '?' {
				// TODO: check 6 or 7 or 0?
				if pathLen > 7 { // any parameters?
					strParams = path[3][7:]
					//log.Println("params", string(strParams))

					params := bytes.Split(strParams, []byte{'&'})
					for _ /*i*/, p := range params {
						//fmt.Println(i, ":", string(p))

						assignment := bytes.IndexByte(p, '=')
						if assignment < 0 {
							ctx.SetStatusCode(fasthttp.StatusBadRequest)
							return nil
						}

						prop = p[:assignment]
						value = p[assignment+1:]
						//fmt.Println(i, string(prop), "=", string(value)) // print parsed parameters from URI

						switch string(prop) {
						case VIS_FROM_DATE:
							// fromDate, err = strconv.ParseInt(string(value), 10, 64)
							fromDate, ok = strToInt64(value)
							if !ok {
								ctx.SetStatusCode(fasthttp.StatusBadRequest)
								return nil
							}
						case VIS_TO_DATE:
							// toDate, err = strconv.ParseInt(string(value), 10, 64)
							toDate, ok = strToInt64(value)
							if !ok {
								ctx.SetStatusCode(fasthttp.StatusBadRequest)
								return nil
							}
						case VIS_COUNTRY:
							country, _ = url.QueryUnescape(string(value))
							if value == nil {
								ctx.SetStatusCode(fasthttp.StatusBadRequest)
								return nil
							}
						case VIS_DISTANCE:
							// toDistance, err = strconv.ParseUint(string(value), 10, 64)
							toDistance, ok = strToInt64(value)
							if !ok {
								ctx.SetStatusCode(fasthttp.StatusBadRequest)
								return nil
							}
						default:
							ctx.SetStatusCode(fasthttp.StatusBadRequest)
							return nil
						}
					}
				}
			}

			arr := u.GetUserVisits(fromDate, toDate, country, toDistance)
			vit := arr.Iterator()

			// TODO: cache in worker or create ReusablePool
			// var body []byte = make([]byte, 0, 256)
			// var body []byte = <-PoolGet
			bbuff := GetBuffer()
			body := bbuff.B

			needComma := false
			body = append(body, VISIT_ARRAY...)
			for vit.Next() {
				v := vit.Value().(*Visit)
				//fmt.Println(time.Unix(v.VisitedAtInt, 0), v.LocationPtr.Country, v.LocationPtr.Distance)

				if needComma {
					body = append(body, COMMA)
				}
				needComma = true

				body = append(body, FBRACET_OPEN)

				body = append(body, MARK_FIELD...)
				body = append(body, strconv.FormatInt(int64(v.MarkInt), 10)...)
				body = append(body, COMMA)

				body = append(body, VISIT_FIELD...)
				body = append(body, strconv.FormatInt(v.VisitedAtInt, 10)...)
				body = append(body, COMMA)

				body = append(body, PLACE_FIELD...)
				body = append(body, '"')
				body = AppendUnicode(body, []byte(v.LocationPtr.Place))
				body = append(body, '"')

				body = append(body, FBRACET_CLOSE)
			}
			body = append(body, VISIT_ARRAY_END...)
			ctx.SetBody(body)

			// PoolPut <- body
			ReleaseBuffer(bbuff)
		}
	}
	return nil
}

func requestLocation(id int64, ctx *fasthttp.RequestCtx, path [][]byte) error {
	l := FindLocation(id, false)
	if l == nil {
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		return nil
	}

	if len(path) == 3 {
		if DEBUG {
			log.Println("@ location request:", id)
		}
		ctx.SetStatusCode(fasthttp.StatusOK)

		// buf := <- PoolGet
		bbuff := GetBuffer()

		buf := l.ToJson(bbuff.B)
		ctx.SetBody(buf)

		// PoolPut <- buf
		ReleaseBuffer(bbuff)

		return nil
	}

	// /locations/<id>/avg
	if len(path) == 4 && path[3] != nil {
		pathLen := len(path[3]) // avg?params...

		if pathLen >= 3 && path[3][0] == 'a' && path[3][2] == 'g' {
			if DEBUG {
				log.Println("@ location avg:", id)
			}

			var strParams []byte
			var fromDate int64 = -2147483648
			toDate := CURRENT_TIME.Unix()
			var fromAge int64 = 0
			var toAge int64 = 1000
			var gender byte

			var prop []byte
			var value []byte
			// var err error
			var ok bool

			if pathLen > 4 && path[3][3] == '?' {
				// TODO: check 4 or 5 or 0?
				if pathLen > 5 { // any parameters?
					strParams = path[3][4:]
					//log.Println("params", string(strParams))

					params := bytes.Split(strParams, []byte{'&'})
					for _ /*i*/, p := range params {
						//fmt.Println(i, ":", string(p))

						assignment := bytes.IndexByte(p, '=')
						if assignment < 0 {
							ctx.SetStatusCode(fasthttp.StatusBadRequest)
							return nil
						}

						prop = p[:assignment]
						value = p[assignment+1:]
						//fmt.Println(i, string(prop), "=", string(value)) // print parsed parameters from URI

						switch string(prop) {
						case LOC_FROM_DATE:
							fromDate, ok = strToInt64(value)
							if !ok {
								ctx.SetStatusCode(fasthttp.StatusBadRequest)
								return nil
							}
						case LOC_TO_DATE:
							toDate, ok = strToInt64(value)
							if !ok {
								ctx.SetStatusCode(fasthttp.StatusBadRequest)
								return nil
							}
						case LOC_FROM_AGE:
							fromAge, ok = strToInt64(value)
							if !ok {
								ctx.SetStatusCode(fasthttp.StatusBadRequest)
								return nil
							}
						case LOC_TO_AGE:
							toAge, ok = strToInt64(value)
							if !ok {
								ctx.SetStatusCode(fasthttp.StatusBadRequest)
								return nil
							}
						case LOC_GENDER:
							switch value[0] {
							case 'm', 'f':
								gender = value[0]
							default:
								//panic("wrong Gender " + string(value))
								ctx.SetStatusCode(fasthttp.StatusBadRequest)
								return nil
							}
						default:
							ctx.SetStatusCode(fasthttp.StatusBadRequest)
							return nil
						}
					}
				}
			}
			avg := l.GetLocationAvg(fromDate, toDate, int(fromAge), int(toAge), gender)

			// TODO: cache in worker or create ReusablePool
			// var body []byte = make([]byte, 0, 32)
			// var body []byte = <-PoolGet
			bbuff := GetBuffer()
			body := append(bbuff.B, LOC_AVG...)

			//body = strconv.AppendFloat(body, avg,'g', 6, 32)
			// FIX: 1
			avg += 1e-10
			body = strconv.AppendFloat(body, avg, 'f', 5, 64)

			body = append(body, LOC_AVG_END...)
			ctx.SetBody(body)

			// PoolPut <- body
			ReleaseBuffer(bbuff)
		}
	}

	return nil
}

func requestVisit(id int64, ctx *fasthttp.RequestCtx, path [][]byte) error {
	if DEBUG {
		log.Println("@ visit request:", id)
	}

	// TODO: check!
	if len(path) > 3 {
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		return nil
	}

	v := FindVisit(id, false)
	if v == nil {
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		return nil
	}
	ctx.SetStatusCode(fasthttp.StatusOK)

	// buf := <- PoolGet
	bbuff := GetBuffer()

	buf := v.ToJson(bbuff.B)
	ctx.SetBody(buf)

	// PoolPut <- buf
	ReleaseBuffer(bbuff)

	return nil
}

//func requestUserVisits(ctx *fasthttp.RequestCtx) error {
//	logRequest(ctx)
//	return nil
//}
//
//func requestAvg(ctx *fasthttp.RequestCtx) error {
//	logRequest(ctx)
//	return nil
//}

const PATH_USER_ID = "users"
const PATH_LOCATION_ID = "locations"
const PATH_VISIT_ID = "visits"

var PostUpdateChan chan IEntity

func createEntity(jsonType int, rawJson []byte, ctx *fasthttp.RequestCtx) {
	//log.Println("### INSERT:", string(rawJson))
	_, res := NewEntity(jsonType, rawJson)
	if res {
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetBody(EMPTY_BODY)
	} else {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
	}
}

func updateEntity(jsonType int, e IEntity, rawJson []byte, ctx *fasthttp.RequestCtx) {
	// e.Print()
	if UpdateEntity(jsonType, e, rawJson) {
		// PostUpdateChan <- e
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetBody(EMPTY_BODY)
		// e.Print()
	} else {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		// e.Print()
	}
}

func postEntity(jsonType int, id int64, ctx *fasthttp.RequestCtx) {
	body := ctx.PostBody()

	if len(body) <= 2 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return
	}

	//log.Println(string(body))

	buf := GetBuffer()
	rawJson, _ := UnquoteBytes(body, buf)
	rawJson = rawJson[:len(rawJson)-1]

	if id == 0 {
		// log.Println("### INSERT:", string(rawJson))
		createEntity(jsonType, rawJson, ctx)
	} else {
		// log.Println("### UPDATE id:", id, string(rawJson))
		var e IEntity
		switch jsonType {
		case USER:
			u := LockFindUser(id, false)
			if u != nil {
				e = u
			}
		case LOCATION:
			l := LockFindLocation(id, false)
			if l != nil {
				e = l
			}
		case VISIT:
			v := LockFindVisit(id, false)
			if v != nil {
				e = v
			}
		}
		if e != nil {
			updateEntity(jsonType, e, rawJson, ctx)
		} else {
			ctx.SetStatusCode(fasthttp.StatusNotFound)
		}
	}
	ReleaseBuffer(buf)
}

var NumberOfActiveRequests int = 0
var debugLock = sync.RWMutex{}

// func TimeoutRequestHandler(ctx *fasthttp.RequestCtx) {
// ch := make(chan bool, 1)
// switch {
// case <-ch:
// return
// case <-time.After(time.Second * 2):
// log.Println("REQUEST TIMEOUT", string(ctx.Request.RequestURI()))
// return
// }
// }

var CacheLock = sync.RWMutex{}
var QueryCache map[string]*fasthttp.ByteBuffer = make(map[string]*fasthttp.ByteBuffer)
var IsCacheUsed bool = true
var UsedFromCache int = 0

const MAX_CACHED_QUERIES = 1024 * 1024 * 1024 / 100

var QueryIDs [MAX_CACHED_QUERIES]string
var queryCacheIndex int = 0

var TankStage int = 0

func CleanCache() {
	log.Println("Clean query cache!")
	CacheLock.Lock()
	IsCacheUsed = false
	for k, b := range QueryCache {
		ReleaseBuffer(b)
		delete(QueryCache, k)
	}
	for i := range QueryIDs {
		QueryIDs[i] = ""
	}
	IsCacheUsed = true
	CacheLock.Unlock()
}

func QueryCacheFindAndUse(queryId string, ctx *fasthttp.RequestCtx) bool {
	CacheLock.Lock()
	cached := QueryCache[queryId]
	CacheLock.Unlock()

	if cached != nil && len(cached.B) > 1 {
		switch cached.B[0] {
		case 'k':
			ctx.SetStatusCode(fasthttp.StatusOK)
		case 'n':
			ctx.SetStatusCode(fasthttp.StatusNotFound)
		case 'b':
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
		}
		ctx.SetBody(cached.B[1:])
		// log.Printf("use cache for: %s, (%d) %s", string(ctx.RequestURI()), ctx.Response.StatusCode(), string(cached.B[1:]))
		UsedFromCache++
		return true
	}
	return false
}

func QueryCacheStore(queryId string, ctx *fasthttp.RequestCtx) {
	CacheLock.Lock()

	// duplicate?
	if QueryCache[queryId] != nil {
		CacheLock.Unlock()
		return
	}

	buf := GetBuffer()
	body := ctx.Response.Body()
	var code byte
	var oldId string

	switch ctx.Response.StatusCode() {
	case fasthttp.StatusOK:
		code = 'k'
	case fasthttp.StatusNotFound:
		code = 'n'
	case fasthttp.StatusBadRequest:
		code = 'b'
	}

	buf.B = append(buf.B, code)
	buf.B = append(buf.B, body...)

	QueryCache[queryId] = buf

	// for i, v := range QueryIDs {
	// log.Println(i, v)
	// }
	// log.Println(".")

	oldId = QueryIDs[queryCacheIndex]
	if len(oldId) > 0 {
		deleteBuf := QueryCache[oldId]
		// log.Println("deleting:", queryCacheIndex, oldId)
		ReleaseBuffer(deleteBuf)
		delete(QueryCache, oldId)
	}
	QueryIDs[queryCacheIndex] = queryId
	queryCacheIndex = (queryCacheIndex + 1) % MAX_CACHED_QUERIES

	CacheLock.Unlock()
}

func RequestHandler(ctx *fasthttp.RequestCtx) {
	if USE_TIMETRACK {
		defer func() {
			TimeTrack(time.Now(), "http "+string(ctx.Request.RequestURI())+" = "+strconv.Itoa(ctx.Response.StatusCode())+" <R> "+strconv.Itoa(NumberOfActiveRequests))
			debugLock.Lock()
			NumberOfActiveRequests--
			debugLock.Unlock()
		}()
	}
	ctx.Response.Header.SetContentType("application/json")
	ctx.Response.Header.Set("Connection", "keep-alive")
	//ctx.Response.Header.SetConnectionClose()

	if DEBUG {
		debugLock.Lock()
		NumberOfActiveRequests++
		debugLock.Unlock()
		log.Println("<R>", NumberOfActiveRequests)
		logRequest(ctx)
	}

	queryId := string(ctx.RequestURI())

	if IsCacheUsed {
		if QueryCacheFindAndUse(queryId, ctx) {
			return
		}
	}

	// TODO: path[0] contains the first '/'
	path := bytes.Split(ctx.RequestURI(), []byte{'/'})

	// shorten path means that it is incorrect
	if len(path) < 3 {
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		return
	}

	idStr := path[2]
	qPos := bytes.IndexByte(idStr, '?')
	if qPos > 0 {
		idStr = idStr[:qPos]
	}

	// id, err := strconv.ParseUint(string(idStr), 10, 64)
	id, ok := strToInt64(idStr)

	// second part should always be an ID
	if ctx.IsPost() {
		if TankStage == 0 {
			TankStage = 1
			go CleanCache()
			MemStat()
			UsedFromCache = 0
		}

		switch string(path[1]) {
		case PATH_USER_ID:
			postEntity(USER, id, ctx)
		case PATH_LOCATION_ID:
			postEntity(LOCATION, id, ctx)
		case PATH_VISIT_ID:
			postEntity(VISIT, id, ctx)
		default:
			ctx.SetStatusCode(fasthttp.StatusNotFound)
		}
	} else {
		if TankStage == 1 {
			TankStage = 2
			MemStat()
			UsedFromCache = 0
		}
		if !ok {
			ctx.SetStatusCode(fasthttp.StatusNotFound)
		} else {
			switch string(path[1]) {
			case PATH_USER_ID:
				requestUser(id, ctx, path)
			case PATH_LOCATION_ID:
				requestLocation(id, ctx, path)
			case PATH_VISIT_ID:
				requestVisit(id, ctx, path)
			default:
				ctx.SetStatusCode(fasthttp.StatusNotFound)
			}
		}
	}
	if IsCacheUsed {
		QueryCacheStore(queryId, ctx)
	}
}
