package main

import (
	"fmt"
	"os"
	"runtime"
	"time"
	//"bufio"
	//"io/ioutil"
	"archive/zip"
	"hash/fnv"
	"log"
	//"strconv"
	"unicode/utf8"
	// "container/list"
	"github.com/valyala/fasthttp"
	"strconv"
	"unicode"
	"unicode/utf16"
)

func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("dT: %s: %s", name, elapsed)
}

func MonitorRuntime() {
	m := &runtime.MemStats{}
	f, err := os.Create(fmt.Sprintf("mmem_%s.csv", time.Now().Format("20170817-10-20-30")))
	if err != nil {
		panic(err)
	}
	f.WriteString("Time;Allocated;Total Allocated; System Memory;Num Gc;Heap Allocated;Heap System;Heap Objects;Heap Released;\n")
	for {
		runtime.ReadMemStats(m)
		f.WriteString(fmt.Sprintf("%s;%d;%d;%d;%d;%d;%d;%d;%d;\n", time.Now().Format(time.RFC3339), m.Alloc, m.TotalAlloc, m.Sys, m.NumGC, m.HeapAlloc, m.HeapSys, m.HeapObjects, m.HeapReleased))
		time.Sleep(1 * time.Second)
	}
}

// https://golangcode.com/unzip-files-in-go/
//func Unzip(src string, unzipChan chan *RawJson) /*([]string, error)*/ {
func Unzip(reader *zip.ReadCloser, unzipChan chan *RawJson) /*([]string, error)*/ {
	if USE_TIMETRACK {
		defer TimeTrack(time.Now(), "Unzipping")
	}

	//r, err := zip.OpenReader(src)
	//if err != nil {
	//	close(unzipChan)
	//	log.Panicln(err)
	//}
	//
	//defer reader.Close()

	for _, f := range reader.File {
		rc, err := f.Open()

		if err != nil {
			close(unzipChan)
			log.Panicln(err)
		}
		//defer rc.Close()

		//log.Println("Unzipping:", f.Name, ", size:", f.UncompressedSize64)

		//reader := bufio.NewReader(rc)
		//rawBytes, err := ioutil.ReadAll(reader)

		unzipChan <- &RawJson{f.Name /*rawBytes*/, rc /*, f.UncompressedSize64*/}
	}
	//close(unzipChan)
	unzipChan <- &RawJson{}
}

func HashS(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func HashB(b []byte) uint32 {
	h := fnv.New32a()
	h.Write(b)
	return h.Sum32()
}

func IsNum(str []byte) bool {
	for _, c := range str {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func strToInt64(s []byte) (res int64, ok bool) {
	sign := len(s) > 0 && s[0] == '-'
	if sign {
		s = s[1:]
	}

	ok = true

	res = 0
	for _, c := range s {
		if v := int64(c - '0'); v < 0 || v > 9 {
			ok = false
			break
		} else {
			res = res*10 + v
		}
	}

	if sign {
		res = -res
	}

	return
}

func AppendUnicode(dst []byte, src []byte) []byte {
	str := string(src)
	runes := []rune(str)
	for _, r := range runes {
		dst = AppendQuoteRuneToASCII(dst, r)
	}
	return dst
}

const lowerhex = "0123456789abcdef"

func AppendQuoteRuneToASCII(dst []byte, r rune) []byte {
	return appendQuotedRuneWith(dst, r, '\'', true, false)
}
func appendQuotedRuneWith(buf []byte, r rune, quote byte, ASCIIonly, graphicOnly bool) []byte {
	//buf = append(buf, quote)
	if !utf8.ValidRune(r) {
		r = utf8.RuneError
	}
	buf = appendEscapedRune(buf, r, utf8.RuneLen(r), quote, ASCIIonly, graphicOnly)
	//buf = append(buf, quote)
	return buf
}

func appendEscapedRune(buf []byte, r rune, width int, quote byte, ASCIIonly, graphicOnly bool) []byte {
	//var runeTmp [utf8.UTFMax]byte
	if r == rune(quote) || r == '\\' { // always backslashed
		buf = append(buf, '\\')
		buf = append(buf, byte(r))
		return buf
	}
	if ASCIIonly {
		if r < utf8.RuneSelf /*&& IsPrint(r)*/ {
			buf = append(buf, byte(r))
			return buf
		}
		//} else if IsPrint(r) || graphicOnly && isInGraphicList(r) {
		//	n := utf8.EncodeRune(runeTmp[:], r)
		//	buf = append(buf, runeTmp[:n]...)
		//	return buf
	}
	switch r {
	case '\a':
		buf = append(buf, `\a`...)
	case '\b':
		buf = append(buf, `\b`...)
	case '\f':
		buf = append(buf, `\f`...)
	case '\n':
		buf = append(buf, `\n`...)
	case '\r':
		buf = append(buf, `\r`...)
	case '\t':
		buf = append(buf, `\t`...)
	case '\v':
		buf = append(buf, `\v`...)
	default:
		switch {
		case r < ' ':
			buf = append(buf, `\x`...)
			buf = append(buf, lowerhex[byte(r)>>4])
			buf = append(buf, lowerhex[byte(r)&0xF])
		case r > utf8.MaxRune:
			r = 0xFFFD
			fallthrough
		case r < 0x10000:
			buf = append(buf, `\u`...)
			for s := 12; s >= 0; s -= 4 {
				buf = append(buf, lowerhex[r>>uint(s)&0xF])
			}
		default:
			buf = append(buf, `\U`...)
			for s := 28; s >= 0; s -= 4 {
				buf = append(buf, lowerhex[r>>uint(s)&0xF])
			}
		}
	}
	return buf
}

func TimeDiff(a, b time.Time) (year, month, day, hour, min, sec int) {
	if a.Location() != b.Location() {
		b = b.In(a.Location())
	}
	if a.After(b) {
		a, b = b, a
	}
	y1, M1, d1 := a.Date()
	y2, M2, d2 := b.Date()

	h1, m1, s1 := a.Clock()
	h2, m2, s2 := b.Clock()

	year = int(y2 - y1)
	month = int(M2 - M1)
	day = int(d2 - d1)
	hour = int(h2 - h1)
	min = int(m2 - m1)
	sec = int(s2 - s1)

	// Normalize negative values
	if sec < 0 {
		sec += 60
		min--
	}
	if min < 0 {
		min += 60
		hour--
	}
	if hour < 0 {
		hour += 24
		day--
	}
	if day < 0 {
		// days in month:
		t := time.Date(y1, M1, 32, 0, 0, 0, 0, time.UTC)
		day += 32 - t.Day()
		month--
	}
	if month < 0 {
		month += 12
		year--
	}

	return
}

var BuffersCount int = 0
var BuffersTimeoutCount int = 0

func GetBuffer() *fasthttp.ByteBuffer {
	BuffersCount++
	if DEBUG {
		fmt.Println("Pool allocation", BuffersCount, BuffersTimeoutCount)
	}
	buf := fasthttp.AcquireByteBuffer()
	if cap(buf.B) == 0 {
		buf.B = make([]byte, 0, 4096)
	}
	return buf
}

func ReleaseBuffer(buf *fasthttp.ByteBuffer) {
	BuffersTimeoutCount++
	if DEBUG {
		fmt.Println("Pool free", BuffersCount, BuffersTimeoutCount)
	}
	fasthttp.ReleaseByteBuffer(buf)
}

// getu4 decodes \uXXXX from the beginning of s, returning the hex value,
// or it returns -1.
func getu4(s []byte) rune {
	if len(s) < 6 || s[0] != '\\' || s[1] != 'u' {
		return -1
	}
	r, err := strconv.ParseUint(string(s[2:6]), 16, 64)
	if err != nil {
		return -1
	}
	return rune(r)
}

func UnquoteBytes(s []byte, buf *fasthttp.ByteBuffer) (t []byte, ok bool) {
	if len(s) < 2 /*|| s[0] != '"' || s[len(s)-1] != '"'*/ {
		return
	}
	//s = s[1 : len(s)-1]

	// Check for unusual characters. If there are none,
	// then no unquoting is needed, so return a slice of the
	// original bytes.
	r := 0
	for r < len(s) {
		c := s[r]

		if c == '\\' || c == '"' || c < ' ' {
			break
		}
		if c < utf8.RuneSelf {
			r++
			continue
		}
		rr, size := utf8.DecodeRune(s[r:])
		if rr == utf8.RuneError && size == 1 {
			break
		}
		r += size
	}
	if r == len(s) {
		return s, true
	}

	if cap(buf.B) < len(s)+2*utf8.UTFMax {
		buf.B = make([]byte, len(s)+2*utf8.UTFMax)
	}
	b := buf.B[:len(s)+2*utf8.UTFMax]

	//b := make([]byte, len(s)+2*utf8.UTFMax)

	w := copy(b, s[0:r])
	for r < len(s) {
		// Out of room?  Can only happen if s is full of
		// malformed UTF-8 and we're replacing each
		// byte with RuneError.
		if w >= len(b)-2*utf8.UTFMax {
			nb := make([]byte, (len(b)+utf8.UTFMax)*2)
			copy(nb, b[0:w])
			b = nb
		}
		switch c := s[r]; {
		case c == '\\':
			r++
			if r >= len(s) {
				return
			}
			switch s[r] {
			default:
				return
			case '"', '\\', '/', '\'':
				b[w] = s[r]
				r++
				w++
			case 'b':
				b[w] = '\b'
				r++
				w++
			case 'f':
				b[w] = '\f'
				r++
				w++
			case 'n':
				b[w] = '\n'
				r++
				w++
			case 'r':
				b[w] = '\r'
				r++
				w++
			case 't':
				b[w] = '\t'
				r++
				w++
			case 'u':
				r--
				rr := getu4(s[r:])
				if rr < 0 {
					return
				}
				r += 6
				if utf16.IsSurrogate(rr) {
					rr1 := getu4(s[r:])
					if dec := utf16.DecodeRune(rr, rr1); dec != unicode.ReplacementChar {
						// A valid pair; consume.
						r += 6
						w += utf8.EncodeRune(b[w:], dec)
						break
					}
					// Invalid surrogate; fall back to replacement rune.
					rr = unicode.ReplacementChar
				}
				w += utf8.EncodeRune(b[w:], rr)
			}

			// Quote, control characters are invalid.
		//case c == '"', c < ' ':
		//	return

		// ASCII
		case c < utf8.RuneSelf:
			b[w] = c
			r++
			w++

			// Coerce to well-formed UTF-8.
		default:
			rr, size := utf8.DecodeRune(s[r:])
			r += size
			w += utf8.EncodeRune(b[w:], rr)
		}
	}
	return b[0:w], true
}

func MemStat() {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	log.Println("--------------------------------")
	switch TankStage {
	case 0:
		log.Println("\tStage 1")
	case 1:
		log.Println("\tStage 2")
	case 2:
		log.Println("\tStage 3")
	}
	log.Println("--------------------------------")
	log.Println("Alloc:\t\t", mem.Alloc/1024/1024, "Mb, bytes:", mem.Alloc)
	log.Println("TotalAlloc:\t\t", mem.TotalAlloc/1024/1024, "Mb, bytes:", mem.TotalAlloc)
	log.Println("HeapAlloc:\t\t", mem.HeapAlloc/1024/1024, "Mb, bytes:", mem.HeapAlloc)
	log.Println("HeapSys:\t\t", mem.HeapSys/1024/1024, "Mb, bytes:", mem.HeapSys)
	log.Println("HeapInuse:\t\t", mem.HeapInuse/1024/1024, "Mb, bytes:", mem.HeapInuse)
	log.Println("HeapReleased:\t", mem.HeapReleased/1024/1024, "Mb, bytes:", mem.HeapReleased)
	log.Println("HeapObjects:\t", mem.HeapObjects)
	log.Println("NumGC:\t\t", mem.NumGC)
	log.Println("BuffersCount:\t\t", BuffersCount)
	log.Println("BuffersTimeoutCount: \t", BuffersTimeoutCount)
	log.Println("CACHE prev. UsedFromCache: \t", UsedFromCache)
	log.Println("--------------------------------")
}
