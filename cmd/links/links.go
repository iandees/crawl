// A restartable crawler that extracts links from HTML pages and
// simply prints them.
//

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"

	"git.autistici.org/ale/crawl"
	"github.com/PuerkitoBio/goquery"
)

var (
	dbPath       = flag.String("state", "crawldb", "crawl state database path")
	concurrency  = flag.Int("c", 10, "concurrent workers")
	depth        = flag.Int("depth", 10, "maximum link depth")
	validSchemes = flag.String("schemes", "http,https", "comma-separated list of allowed protocols")
)

var linkMatches = []struct {
	tag  string
	attr string
}{
	{"a", "href"},
	{"link", "href"},
	{"img", "src"},
	{"script", "src"},
}

func extractLinks(c *crawl.Crawler, u string, depth int, resp *http.Response, err error) error {
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html") {
		return nil
	}

	doc, err := goquery.NewDocumentFromResponse(resp)
	if err != nil {
		return err
	}

	links := make(map[string]*url.URL)

	for _, lm := range linkMatches {
		doc.Find(fmt.Sprintf("%s[%s]", lm.tag, lm.attr)).Each(func(i int, s *goquery.Selection) {
			val, _ := s.Attr(lm.attr)
			if linkurl, err := resp.Request.URL.Parse(val); err == nil {
				links[linkurl.String()] = linkurl
			}
		})
	}

	for _, link := range links {
		//log.Printf("%s -> %s", u, link.String())
		c.Enqueue(link, depth+1)
	}
	return nil
}

func main() {
	flag.Parse()

	seeds := crawl.MustParseURLs(flag.Args())
	scope := crawl.NewSeedScope(seeds, *depth, strings.Split(*validSchemes, ","))

	crawler, err := crawl.NewCrawler("crawldb", seeds, scope, crawl.FetcherFunc(http.Get), crawl.HandlerFunc(extractLinks))
	if err != nil {
		log.Fatal(err)
	}
	crawler.Run(*concurrency)
}
