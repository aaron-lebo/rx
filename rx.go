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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var history map[string]record

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
	f, err := os.Open("out/history.csv")
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
    return fmt.Sprintf("%.5f", f1 / f2)
}

func strSize(size float64) string {
	if size > 1000 {
		return fmt.Sprintf("%.2f GB", size/1000.0)
	}
	return fmt.Sprintf("%.2f MB", size)
}

func saveHistory(keywords []string) (sum record) {
	files := make([]string, len(history))
	var i int
	for f := range history {
		files[i] = f
		i++
	}
	sort.Strings(files)

	f, err := os.Create("out/history.csv")
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
			str(r.SizeIn), str(r.SizeOut), str64(r.SizeOut, r.SizeIn),
			str(r.NumIn), str(r.NumOut), str64(float64(r.NumOut), float64(r.NumIn)),
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
		strSize(sum.SizeIn), strSize(sum.SizeOut), str64(sum.SizeOut, sum.SizeIn),
		str(sum.NumIn), str(sum.NumOut), str64(float64(sum.NumOut), float64(sum.NumIn)),
	}, counts...))
	return
}

func runLine(line []byte, comments bool, keywords [][]*regexp.Regexp, enc *json.Encoder, rec *record) {
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
	text = strings.ToLower(text)
	for i, k := range keywords {
		match := true
		for _, k := range k {
			match = match && k.MatchString(text)
		}
		if match {
			enc.Encode(obj)
			rec.NumOut++
			rec.Counts[i]++
			break
		}
	}
}

func runFile(file string, keywords [][]*regexp.Regexp) {
	start := time.Now()
	f, err := os.Open(file)
	check(err)
	defer f.Close()
	r := bufio.NewReader(f)
	if filepath.Ext(file) == ".bz2" {
		r = bufio.NewReader(bzip2.NewReader(r))
	} else {
		r1, err := xz.NewReader(r)
		check(err)
		r = bufio.NewReader(r1)
	}

	comments := strings.Contains(file, "RC_")

	_, file = filepath.Split(f.Name())
	f1, err := os.Create(fmt.Sprintf("out/%v.xz", strings.Split(file, ".")[0]))
	check(err)
	defer f1.Close()
	w, err := xz.NewWriter(bufio.NewWriter(f1))
	check(err)
	defer w.Close()
	enc := json.NewEncoder(w)

	rec := record{Counts: make([]int, len(keywords))}
	lastTime := time.Now()
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			break
		}
		rec.NumIn++
		runLine(line, comments, keywords, enc, &rec)
		if time.Since(lastTime).Seconds() > 1.0 {
			fmt.Printf("%v %v %v   \r", file, str(time.Since(start)), rec.NumIn)
			lastTime = time.Now()
		}
	}

	rec.Time = time.Since(start)
	sq := 1000.0 * 1000.0
	stat, err := f.Stat()
	check(err)
	rec.SizeIn = float64(stat.Size()) / sq
	w.Close()
	stat, err = f1.Stat()
	check(err)
	rec.SizeOut = float64(stat.Size()) / sq
	history[file] = rec
	fmt.Println(file, str(rec.Time), rec.NumIn)
}

func main() {
	os.MkdirAll("out", 0755)
	loadHistory()

	var sum record
	files, err := filepath.Glob(os.Args[1])
	check(err)
	keywords := strings.Split(strings.ToLower(os.Args[2]), ",")
	for i, k := range keywords {
		keywords[i] = strings.TrimSpace(k)
	}
	splitKeywords := make([][]*regexp.Regexp, len(keywords))
	for i, k := range keywords {
		parts := strings.Split(k, " and ")
		splitKeywords[i] = make([]*regexp.Regexp, len(parts))
		for i1, p := range parts {
			splitKeywords[i][i1] = regexp.MustCompile(`\b` + p + `\b`)
		}
	}
	for _, f := range files {
		runFile(f, splitKeywords)
		sum = saveHistory(keywords)
	}
	fmt.Println("*", str(sum.Time), str(sum.NumIn))
}
