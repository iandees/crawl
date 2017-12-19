// A restartable crawler that dumps everything to a WARC file.

package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"git.autistici.org/ale/crawl"
	"git.autistici.org/ale/crawl/analysis"
	"git.autistici.org/ale/crawl/warc"
)

var (
	dbPath         = flag.String("state", "crawldb", "crawl state database path")
	keepDb         = flag.Bool("keep", false, "keep the state database when done")
	concurrency    = flag.Int("c", 10, "concurrent workers")
	depth          = flag.Int("depth", 100, "maximum link depth")
	validSchemes   = flag.String("schemes", "http,https", "comma-separated list of allowed protocols")
	excludeRelated = flag.Bool("exclude-related", false, "include related resources (css, images, etc) only if their URL is in scope")
	outputFile     = flag.String("output", "crawl.warc.gz", "output WARC file")

	cpuprofile = flag.String("cpuprofile", "", "create cpu profile")
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

type fakeCloser struct {
	io.Reader
}

func (f *fakeCloser) Close() error {
	return nil
}

func hdr2str(h http.Header) []byte {
	var b bytes.Buffer
	h.Write(&b)
	return b.Bytes()
}

type warcSaveHandler struct {
	warc       *warc.Writer
	warcInfoID string
}

func (h *warcSaveHandler) Handle(c *crawl.Crawler, u string, depth int, resp *http.Response, err error) error {
	data, derr := ioutil.ReadAll(resp.Body)
	if derr != nil {
		return err
	}
	resp.Body = &fakeCloser{bytes.NewReader(data)}

	// Dump the request.
	var b bytes.Buffer
	resp.Request.Write(&b)
	hdr := warc.NewHeader()
	hdr.Set("WARC-Type", "request")
	hdr.Set("WARC-Target-URI", resp.Request.URL.String())
	hdr.Set("WARC-Warcinfo-ID", h.warcInfoID)
	hdr.Set("Content-Length", strconv.Itoa(b.Len()))
	w := h.warc.NewRecord(hdr)
	w.Write(b.Bytes())
	w.Close()

	// Dump the response.
	statusLine := fmt.Sprintf("HTTP/1.1 %s", resp.Status)
	respPayload := bytes.Join([][]byte{
		[]byte(statusLine), hdr2str(resp.Header), data},
		[]byte{'\r', '\n'})
	hdr = warc.NewHeader()
	hdr.Set("WARC-Type", "response")
	hdr.Set("WARC-Target-URI", resp.Request.URL.String())
	hdr.Set("WARC-Warcinfo-ID", h.warcInfoID)
	hdr.Set("Content-Length", strconv.Itoa(len(respPayload)))
	w = h.warc.NewRecord(hdr)
	w.Write(respPayload)
	w.Close()

	return extractLinks(c, u, depth, resp, err)
}

func newWarcSaveHandler(w *warc.Writer) crawl.Handler {
	info := strings.Join([]string{
		"Software: crawl/1.0\r\n",
		"Format: WARC File Format 1.0\r\n",
		"Conformsto: http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf\r\n",
	}, "")

	hdr := warc.NewHeader()
	hdr.Set("WARC-Type", "warcinfo")
	hdr.Set("WARC-Warcinfo-ID", hdr.Get("WARC-Record-ID"))
	hdr.Set("Content-Length", strconv.Itoa(len(info)))
	hdrw := w.NewRecord(hdr)
	io.WriteString(hdrw, info)
	hdrw.Close()
	return &warcSaveHandler{
		warc:       w,
		warcInfoID: hdr.Get("WARC-Record-ID"),
	}
}

type crawlStats struct {
	bytes int64
	start time.Time

	lock   sync.Mutex
	states map[int]int
}

func (c *crawlStats) Update(resp *http.Response) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.states[resp.StatusCode]++
	resp.Body = &byteCounter{resp.Body}
}

func (c *crawlStats) UpdateBytes(n int64) {
	atomic.AddInt64(&c.bytes, n)
}

func (c *crawlStats) Dump() {
	c.lock.Lock()
	defer c.lock.Unlock()
	rate := float64(c.bytes) / time.Since(c.start).Seconds() / 1000
	fmt.Fprintf(os.Stderr, "stats: downloaded %d bytes (%.4g KB/s), status: %v\n", c.bytes, rate, c.states)
}

var stats *crawlStats

func fetch(urlstr string) (*http.Response, error) {
	resp, err := crawl.DefaultClient.Get(urlstr)
	if err == nil {
		stats.Update(resp)
	}
	return resp, err
}

func init() {
	stats = &crawlStats{
		states: make(map[int]int),
		start:  time.Now(),
	}

	go func() {
		for range time.Tick(10 * time.Second) {
			stats.Dump()
		}
	}()
}

type byteCounter struct {
	io.ReadCloser
}

func (b *byteCounter) Read(buf []byte) (int, error) {
	n, err := b.ReadCloser.Read(buf)
	if n > 0 {
		stats.UpdateBytes(int64(n))
	}
	return n, err
}

func main() {
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	outf, err := os.Create(*outputFile)
	if err != nil {
		log.Fatal(err)
	}

	seeds := crawl.MustParseURLs(flag.Args())
	scope := crawl.AND(
		crawl.NewSchemeScope(strings.Split(*validSchemes, ",")),
		crawl.NewDepthScope(*depth),
		crawl.NewSeedScope(seeds),
		crawl.NewRegexpIgnoreScope(nil),
	)
	if !*excludeRelated {
		scope = crawl.OR(scope, crawl.NewIncludeRelatedScope())
	}

	w := warc.NewWriter(outf)
	defer w.Close()

	saver := newWarcSaveHandler(w)

	crawler, err := crawl.NewCrawler(*dbPath, seeds, scope, crawl.FetcherFunc(fetch), crawl.NewRedirectHandler(saver))
	if err != nil {
		log.Fatal(err)
	}

	// Set up signal handlers so we can terminate gently if possible.
	var signaled atomic.Value
	signaled.Store(false)
	sigCh := make(chan os.Signal, 1)
	go func() {
		<-sigCh
		log.Printf("exiting due to signal")
		signaled.Store(true)
		crawler.Stop()
	}()
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	crawler.Run(*concurrency)

	crawler.Close()

	if signaled.Load().(bool) {
		os.Exit(1)
	}
	if !*keepDb {
		os.RemoveAll(*dbPath)
	}
}
