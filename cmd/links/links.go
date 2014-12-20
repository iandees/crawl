// A restartable crawler that extracts links from HTML pages and
// simply prints them.
//

package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"git.autistici.org/ale/crawl"
	"git.autistici.org/ale/crawl/analysis"
)

var (
	dbPath       = flag.String("state", "crawldb", "crawl state database path")
	concurrency  = flag.Int("c", 10, "concurrent workers")
	depth        = flag.Int("depth", 10, "maximum link depth")
	validSchemes = flag.String("schemes", "http,https", "comma-separated list of allowed protocols")
)

func extractLinks(c *crawl.Crawler, u string, depth int, resp *http.Response, err error) error {
	links, err := analysis.GetLinks(resp)
	if err != nil {
		return err
	}

	for _, link := range links {
		c.Enqueue(link, depth+1)
	}

	return nil
}

func main() {
	flag.Parse()

	seeds := crawl.MustParseURLs(flag.Args())
	scope := []crawl.Scope{
		crawl.NewSchemeScope(strings.Split(*validSchemes, ",")),
		crawl.NewDepthScope(*depth),
		crawl.NewSeedScope(seeds),
	}

	crawler, err := crawl.NewCrawler("crawldb", seeds, scope, crawl.FetcherFunc(http.Get), crawl.HandlerFunc(extractLinks))
	if err != nil {
		log.Fatal(err)
	}
	crawler.Run(*concurrency)
}
