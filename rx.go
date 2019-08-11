package main

import (
	"fmt"
	"github.com/NYTimes/gziphandler"
	"net/http"
	"regexp"
)

var unsafe *regexp.Regexp

func init() {
	unsafe = regexp.MustCompile("[^\\w ]")
}

func getArg(r *http.Request, key, default_val string) string {
	val := default_val
	if vals, ok := r.URL.Query()[key]; ok {
		val = vals[0]
	}
	return val
}

func index(w http.ResponseWriter, r *http.Request) {
	date := getArg(r, "date", "")
	subreddit := getArg(r, "subreddit", "")
	page := getArg(r, "page", "1")
	filename := unsafe.ReplaceAllString(fmt.Sprintf("%s_%s_%s", date, subreddit, page), "_")
	http.ServeFile(w, r, fmt.Sprintf("static/%s.html", filename))
}

func main() {
	http.Handle("/", gziphandler.GzipHandler(http.HandlerFunc(index)))
	http.ListenAndServe(":80", nil)
}
