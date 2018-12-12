package main

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

var (
	keywords []string
	dir      string
	history  map[string]record
)

type record struct {
	Time            time.Duration
	SizeIn, SizeOut float64
	NumIn, NumOut   int
	Counts          []int
}

type thing struct {
	Id          string      `json:"id"`
	Author      string      `json:"author"`
	Subreddit   string      `json:"subreddit"`
	CreatedUTC  interface{} `json:"created_utc"`
	RetrievedOn int         `json:"retrieved_on"`
	Edited      interface{} `json:"edited"`
}

type comment struct {
	thing
	Body     string `json:"body"`
	LinkId   string `json:"link_id"`
	ParentId string `json:"parent_id"`
}

type submission struct {
	thing
	Title    string `json:"title"`
	Domain   string `json:"domain"`
	Selftext string `json:"selftext,omitempty"`
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func loadHistory() {
	history = make(map[string]record)
	f, err := os.Open(dir + "history.csv")
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
		r := record{}
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

func str(val interface{}) string {
	switch v := val.(type) {
	case float32:
		return fmt.Sprintf("%.4f", v)
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

func strSize(size float64) string {
	if size > 1000 {
		return fmt.Sprintf("%.2f GB", size/1000.0)
	}
	return fmt.Sprintf("%.2f MB", size)
}

func saveHistory() (sum record) {
	files := make([]string, len(history))
	var i int
	for f := range history {
		files[i] = f
		i++
	}
	sort.Strings(files)

	f, err := os.Create(dir + "history.csv")
	check(err)
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write(append([]string{
		"file", "time",
		"size in (MB)", "size out (MB)", "size ratio",
		"# in", "# out", "# ratio",
	}, keywords...))

	sum.Counts = make([]int, len(keywords))
	for _, f := range files {
		r := history[f]
		counts := make([]string, len(keywords))
		for i, n := range r.Counts {
			counts[i] = strconv.Itoa(n)
			sum.Counts[i] += n
		}
		w.Write(append([]string{
			f, str(r.Time),
			str(r.SizeIn), str(r.SizeOut), str(float32(r.SizeOut / r.SizeIn)),
			str(r.NumIn), str(r.NumOut), str(float32(r.NumOut) / float32(r.NumIn)),
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
	w.Write(append([]string{
		"", str(sum.Time),
		strSize(sum.SizeIn), strSize(sum.SizeOut), str(float32(sum.SizeOut / sum.SizeIn)),
		str(sum.NumIn), str(sum.NumOut), str(float32(sum.NumOut) / float32(sum.NumIn)),
	}, counts...))
	return
}

func contains(strs []string, str string) bool {
	for _, s := range strs {
		if str == s {
			return true
		}
	}
	return false
}

func runLine(line []byte, enc *json.Encoder, comments bool, rec *record) {
	var text string
	var obj interface{}
	if comments {
		var c comment
		check(json.Unmarshal(line, &c))
		c.Body = strings.TrimSpace(c.Body)
		text = c.Body
		obj = c
	} else {
		var s submission
		check(json.Unmarshal(line, &s))
		s.Title = strings.TrimSpace(s.Title)
		s.Selftext = strings.TrimSpace(s.Selftext)
		text = s.Title + s.Selftext
		obj = s
	}
	words := strings.Fields(strings.ToLower(text))
	for i, k := range keywords {
		match := true
		for _, k := range strings.Split(k, " and ") {
			match = match && contains(words, k)
		}
		if match {
			enc.Encode(obj)
			rec.NumOut++
			rec.Counts[i]++
			break
		}
	}
}

func runFile(file string) {
	start := time.Now()
	f, err := os.Open(file)
	check(err)
	defer f.Close()
	r := bufio.NewReader(f)
	r = bufio.NewReader(bzip2.NewReader(r))

	_, file = filepath.Split(f.Name())
	//ext := filepath.Ext(file)
	file = strings.Split(file, ".")[0]
	fOut, err := os.Create(dir + file + ".xz")
	check(err)
	defer fOut.Close()
	w, err := xz.NewWriter(bufio.NewWriter(fOut))
	check(err)
	defer w.Close()
	enc := json.NewEncoder(w)

	comments := strings.Contains(file, "RC_")
	rec := record{Counts: make([]int, len(keywords))}
	lastTime := time.Now()
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			break
		}
		rec.NumIn++
		runLine(line, enc, comments, &rec)
		if time.Since(lastTime).Seconds() > 1.0 {
			fmt.Printf("%v %v %v   \r", file, str(time.Since(start)), rec.NumIn)
			lastTime = time.Now()
		}
	}

	rec.Time = time.Since(start)
	sq := 1000.0 * 1000.0
	fStat, err := f.Stat()
	check(err)
	rec.SizeIn = float64(fStat.Size()) / sq
	w.Close()
	fOutStat, err := fOut.Stat()
	check(err)
	rec.SizeOut = float64(fOutStat.Size()) / sq
	history[file] = rec
	fmt.Println(file, str(rec.Time), rec.NumIn)
}

func main() {
	keywords = strings.Split(strings.ToLower(os.Args[2]), ",")
	for i, k := range keywords {
		keywords[i] = strings.TrimSpace(k)
	}

	dir = "out/" + strings.Join(keywords, "-") + "/"
	os.MkdirAll(dir, 0755)
	loadHistory()

	var sum record
	files, err := filepath.Glob(os.Args[1])
	check(err)
	for _, f := range files {
		runFile(f)
		sum = saveHistory()
	}
	fmt.Println("*", str(sum.Time), str(sum.NumIn))
}
