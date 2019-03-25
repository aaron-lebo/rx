package lib

import (
	"bufio"
	"compress/bzip2"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/ulikunitz/xz"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

var history map[string]Record

type Thing struct {
	Id          string      `json:"id"`
	Author      string      `json:"author"`
	Subreddit   string      `json:"subreddit"`
	CreatedUTC  interface{} `json:"created_utc"`
	RetrievedOn int         `json:"retrieved_on"`
	Edited      interface{} `json:"edited"`
}

type Comment struct {
	Thing
	Body     string `json:"body"`
	LinkId   string `json:"link_id"`
	ParentId string `json:"parent_id"`
}

type Submission struct {
	Thing
	Title    string `json:"title"`
	Domain   string `json:"domain"`
	Selftext string `json:"selftext,omitempty"`
}

type Record struct {
	File            string
	Start           time.Time
	Time            time.Duration
	SizeIn, SizeOut float64
	NumIn, NumOut   int
	Counts          []int
}

func Check(err error) {
	if err != nil {
		panic(err)
	}
}

func LoadHistory(command string) {
	history = make(map[string]Record)
	f, err := os.Open("out/" + command + "/history.csv")
	if err != nil {
		return
	}
	defer f.Close()
	r := csv.NewReader(f)
	row, _ := r.Read()
	for {
		row, _ = r.Read()
		if row[0] == "" {
			break
		}
		r := Record{}
		r.Time, _ = time.ParseDuration(row[1])
		r.SizeIn, _ = strconv.ParseFloat(row[2], 64)
		r.SizeOut, _ = strconv.ParseFloat(row[3], 64)
		r.NumIn, _ = strconv.Atoi(row[5])
		r.NumOut, _ = strconv.Atoi(row[6])
		r.Counts = make([]int, len(row)-8)
		for i, n := range row[8:] {
			r.Counts[i], _ = strconv.Atoi(n)
		}
		history[row[0]] = r
	}
}

func Str(val interface{}) string {
	switch v := val.(type) {
	case float64:
		return fmt.Sprintf("%.2f", v)
	case time.Duration:
		h := int64(math.Mod(v.Hours(), 24))
		m := int64(math.Mod(v.Minutes(), 60))
		s := int64(math.Mod(v.Seconds(), 60))
		if h > 0 {
			return fmt.Sprintf("%dh%dm%ds", h, m, s)
		} else if m > 0 {
			return fmt.Sprintf("%dm%ds", m, s)
		}
		return fmt.Sprintf("%ds", s)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func str64(f1, f2 float64) string {
	return fmt.Sprintf("%.5f", f1/f2)
}

func strSize(size float64) string {
	if size > 1000 {
		return fmt.Sprintf("%.2f GB", size/1000.0)
	}
	return fmt.Sprintf("%.2f MB", size)
}

func SaveHistory(f, f1 *os.File, w *xz.Writer, rec, sum *Record, command string, keywords []string) {
	sq := 1000.0 * 1000.0
	rec.Time = time.Since(rec.Start)
	stat, err := f.Stat()
	Check(err)
	rec.SizeIn = float64(stat.Size()) / sq
    w.Close()
	stat, err = f1.Stat()
	Check(err)
	rec.SizeOut = float64(stat.Size()) / sq
	history[rec.File] = *rec
	fmt.Println(rec.File, Str(rec.Time), rec.NumIn)

    files := make([]string, len(history))
	var i int
	for file := range history {
		files[i] = file
		i++
	}
	sort.Strings(files)

	f2, err := os.Create("out/" + command + "/history.csv")
	Check(err)
	defer f2.Close()
	w1 := csv.NewWriter(f2)
	defer w1.Flush()
	w1.Write(append([]string{
		"file", "time",
		"size in (MB)", "size out (MB)", "size ratio",
		"# in", "# out", "# ratio",
	}, keywords...))

	sum.Counts = make([]int, len(keywords))
	for _, file := range files {
		r := history[file]
		counts := make([]string, len(keywords))
		for i, n := range r.Counts {
			counts[i] = strconv.Itoa(n)
			sum.Counts[i] += n
		}
		w1.Write(append([]string{
			file, Str(r.Time),
			Str(r.SizeIn), Str(r.SizeOut), str64(r.SizeOut, r.SizeIn),
			Str(r.NumIn), Str(r.NumOut), str64(float64(r.NumOut), float64(r.NumIn)),
		}, counts...))

		sum.Time += r.Time
		sum.SizeIn += r.SizeIn
		sum.SizeOut += r.SizeOut
		sum.NumIn += r.NumIn
		sum.NumOut += r.NumOut
	}
	counts := make([]string, len(sum.Counts))
	for i, n := range sum.Counts {
		counts[i] = strconv.Itoa(n)
	}

	w1.Write(append([]string{
		"", Str(sum.Time),
		strSize(sum.SizeIn), strSize(sum.SizeOut), str64(sum.SizeOut, sum.SizeIn),
		Str(sum.NumIn), Str(sum.NumOut), str64(float64(sum.NumOut), float64(sum.NumIn)),
	}, counts...))
}

func Open(file string) (*os.File, *bufio.Reader, bool) {
	f, err := os.Open(file)
	Check(err)
	r := bufio.NewReader(f)
	if filepath.Ext(file) == ".bz2" {
		r = bufio.NewReader(bzip2.NewReader(r))
	} else {
		r1, err := xz.NewReader(r)
		Check(err)
		r = bufio.NewReader(r1)
	}
	return f, r, strings.Contains(file, "RC_")
}

func OpenWriter(f *os.File, command string) (*os.File, *xz.Writer, *json.Encoder, Record) {
	_, file := filepath.Split(f.Name())
	f1, err := os.Create(fmt.Sprintf("out/%s/%v.xz", command, strings.Split(file, ".")[0]))
	Check(err)
	w, err := xz.NewWriter(bufio.NewWriter(f1))
	Check(err)
	return f1, w, json.NewEncoder(w), Record{File: file, Start: time.Now()}
}

func ReadLine(r *bufio.Reader, fn func([]byte)) {
    for {
        line, err := r.ReadBytes('\n')
        if err != nil {
            break
        }
        fn(line)
    }
}

func UpdateProgress(rec *Record) {
	since1 := time.Since(rec.Start)
	rec.NumIn++
	if (since1 - rec.Time).Seconds() > 1.0 {
		fmt.Printf("%v %v %v   \r", rec.File, Str(since1), rec.NumIn)
		rec.Time = since1
	}
}
