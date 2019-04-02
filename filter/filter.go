package filter

import (
	"encoding/json"
	"fmt"
	. "github.com/aaron-lebo/rx/lib"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func Run() {
	files, err := filepath.Glob(os.Args[2])
	Check(err)
	keywords := strings.Split(strings.ToLower(os.Args[3]), ",")
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
	var sum Record
	for _, file := range files {
		f, r, is_rc := Open(file)
		f1, w, enc, rec := OpenWriter(f, "filter")
		defer f.Close()
		defer f1.Close()
		rec.Counts = make([]int, len(splitKeywords))
		ReadLine(r, func(line []byte) {
			var text string
			var obj interface{}
			if is_rc {
				var c Comment
				Check(json.Unmarshal(line, &c))
				c.Body = strings.TrimSpace(c.Body)
				text = c.Body
				obj = c
			} else {
				var s Submission
				Check(json.Unmarshal(line, &s))
				s.Title = strings.TrimSpace(s.Title)
				s.Selftext = strings.TrimSpace(s.Selftext)
				text = s.Title + s.Selftext
				obj = s
			}
			text = strings.ToLower(text)
			for i, k := range splitKeywords {
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
			UpdateProgress(rec)
		})
		sum = SaveHistory(f, f1, w, rec, "filter", keywords)
	}
	fmt.Println("*", Str(sum.Time), Str(sum.NumIn))
}
