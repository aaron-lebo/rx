package collect

import (
	"encoding/json"
	"fmt"
	. "github.com/aaron-lebo/rx/lib"
	"os"
	"path/filepath"
)

var comments, submissions map[string]bool

func init() {
	comments = make(map[string]bool)
	submissions = make(map[string]bool)
}

func readFiles(pattern string) {
	files, err := filepath.Glob(pattern)
	Check(err)
	for _, file := range files {
		fmt.Println("reading", file)
		f, r, is_rc := Open(file)
		defer f.Close()
		ReadLine(r, func(line []byte) {
			var t Thing
			Check(json.Unmarshal(line, &t))
			if is_rc {
				comments["t1_"+t.Id] = true
			} else {
				submissions["t3_"+t.Id] = true
			}
		})
	}
}

func Run() {
	readFiles("out/**/RC*")
	readFiles("out/**/RS*")

	files, err := filepath.Glob(os.Args[2] + "/RC*")
	Check(err)
	sum := &Record{}
	for _, file := range files {
		f, r, _ := Open(file)
		f1, w, enc, rec := OpenWriter(f, "collect")
		defer f.Close()
		defer f1.Close()
		ReadLine(r, func(line []byte) {
			var c Comment
			Check(json.Unmarshal(line, &c))
			id := "t1_" + c.Id
			_, ok := comments[id]
			_, ok1 := comments[c.ParentId]
			_, ok2 := submissions[c.LinkId]
			if ok || ok1 || ok2 {
				comments[id] = true
				enc.Encode(c)
				rec.NumOut++
			}
			UpdateProgress(rec)
		})
		SaveHistory(f, f1, w, rec, sum, "collect", []string{})
	}
	fmt.Println("*", Str(sum.Time), Str(sum.NumIn))
}
