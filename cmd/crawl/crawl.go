// A restartable crawler that dumps everything to a WARC file.

package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
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
	excludeRelated = flag.Bool("exclude-related", false, "do not include related resources (css, images, etc) if their URL is not in scope")
	outputFile     = flag.String("output", "crawl.warc.gz", "output WARC file or pattern (patterns must include a \"%s\" literal token)")
	warcFileSizeMB = flag.Int("output-max-size", 100, "maximum output WARC file size (in MB) when using patterns")
	cpuprofile     = flag.String("cpuprofile", "", "create cpu profile")
	onlyPrefixes   = flag.String("only-prefixes", "", "comma-separated list of allowed URL prefixes")
	inputFile      = flag.String("input-file", "", "path to a file with URLs to fetch")

	dnsMap   = dnsMapFlag(make(map[string]string))
	excludes []*regexp.Regexp

	httpClient = crawl.DefaultClient
)

func init() {
	flag.Var(&excludesFlag{}, "exclude", "exclude regex URL patterns")
	flag.Var(&excludesFileFlag{}, "exclude-from-file", "load exclude regex URL patterns from a file")
	flag.Var(dnsMap, "resolve", "set DNS overrides (in hostname=addr format)")

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

type excludesFlag struct{}

func (f *excludesFlag) String() string { return "" }

func (f *excludesFlag) Set(s string) error {
	rx, err := regexp.Compile(s)
	if err != nil {
		return err
	}
	excludes = append(excludes, rx)
	return nil
}

type excludesFileFlag struct{}

func (f *excludesFileFlag) String() string { return "" }

func (f *excludesFileFlag) Set(s string) error {
	ff, err := os.Open(s) // #nosec
	if err != nil {
		return err
	}
	defer ff.Close() // nolint
	var lineNum int
	scanner := bufio.NewScanner(ff)
	for scanner.Scan() {
		lineNum++
		rx, err := regexp.Compile(scanner.Text())
		if err != nil {
			return fmt.Errorf("%s, line %d: %v", s, lineNum, err)
		}
		excludes = append(excludes, rx)
	}
	return nil
}

type dnsMapFlag map[string]string

func (f dnsMapFlag) String() string { return "" }

func (f dnsMapFlag) Set(s string) error {
	parts := strings.Split(s, "=")
	if len(parts) != 2 {
		return errors.New("value not in host=addr format")
	}
	f[parts[0]] = parts[1]
	return nil
}

func extractLinks(p crawl.Publisher, u string, depth int, resp *http.Response, _ error) error {
	links, err := analysis.GetLinks(resp)
	if err != nil {
		// This is not a fatal error, just a bad web page.
		return nil
	}

	for _, link := range links {
		if err := p.Enqueue(link, depth+1); err != nil {
			return err
		}
	}

	return nil
}

func hdr2str(h http.Header) []byte {
	var b bytes.Buffer
	h.Write(&b) // nolint
	return b.Bytes()
}

type warcSaveHandler struct {
	warc       *warc.Writer
	warcInfoID string
	numWritten int
}

func (h *warcSaveHandler) writeWARCRecord(typ, uri string, data []byte) error {
	hdr := warc.NewHeader()
	hdr.Set("WARC-Type", typ)
	hdr.Set("WARC-Target-URI", uri)
	hdr.Set("WARC-Warcinfo-ID", h.warcInfoID)
	hdr.Set("Content-Length", strconv.Itoa(len(data)))

	w, err := h.warc.NewRecord(hdr)
	if err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	return w.Close()
}

func (h *warcSaveHandler) Handle(p crawl.Publisher, u string, tag, depth int, resp *http.Response, _ error) error {
	// Read the response body (so we can save it to the WARC
	// output) and replace it with a buffer.
	data, derr := ioutil.ReadAll(resp.Body)
	if derr != nil {
		// Errors at this stage are usually transport-level errors,
		// and as such, retriable.
		return crawl.ErrRetryRequest
	}
	resp.Body = ioutil.NopCloser(bytes.NewReader(data))

	// Dump the request to the WARC output.
	var b bytes.Buffer
	if werr := resp.Request.Write(&b); werr != nil {
		return werr
	}
	if werr := h.writeWARCRecord("request", resp.Request.URL.String(), b.Bytes()); werr != nil {
		return werr
	}

	// Dump the response.
	statusLine := fmt.Sprintf("HTTP/1.1 %s", resp.Status)
	respPayload := bytes.Join(
		[][]byte{[]byte(statusLine), hdr2str(resp.Header), data},
		[]byte{'\r', '\n'},
	)
	if werr := h.writeWARCRecord("response", resp.Request.URL.String(), respPayload); werr != nil {
		return werr
	}

	h.numWritten++

	return extractLinks(p, u, depth, resp, nil)
}

func newWarcSaveHandler(w *warc.Writer) (crawl.Handler, error) {
	info := strings.Join([]string{
		"Software: crawl/1.0\r\n",
		"Format: WARC File Format 1.0\r\n",
		"Conformsto: http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf\r\n",
	}, "")

	hdr := warc.NewHeader()
	hdr.Set("WARC-Type", "warcinfo")
	hdr.Set("WARC-Warcinfo-ID", hdr.Get("WARC-Record-ID"))
	hdr.Set("Content-Length", strconv.Itoa(len(info)))
	hdrw, err := w.NewRecord(hdr)
	if err != nil {
		return nil, err
	}
	if _, err := io.WriteString(hdrw, info); err != nil {
		return nil, err
	}
	hdrw.Close() // nolint
	return &warcSaveHandler{
		warc:       w,
		warcInfoID: hdr.Get("WARC-Record-ID"),
	}, nil
}

type crawlStats struct {
	urls  int64
	bytes int64
	start time.Time

	lock   sync.Mutex
	states map[int]int
}

func (c *crawlStats) Update(resp *http.Response) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.urls++
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
	log.Printf("stats: downloaded %d urls, %d bytes (%.4g KB/s), status: %v", c.urls, c.bytes, rate, c.states) // nolint
}

var stats *crawlStats

func fetch(urlstr string) (*http.Response, error) {
	resp, err := httpClient.Get(urlstr)
	if err == nil {
		stats.Update(resp)
	}
	return resp, err
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

func warcWriterFromFlags() (w *warc.Writer, err error) {
	if strings.Contains(*outputFile, "%s") {
		w, err = warc.NewMultiWriter(*outputFile, uint64(*warcFileSizeMB)*1024*1024)
	} else {
		var f *os.File
		f, err = os.Create(*outputFile)
		if err == nil {
			w = warc.NewWriter(f)
		}
	}
	return
}

func main() {
	log.SetFlags(0)
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

	seeds := crawl.MustParseURLs(flag.Args())

	prefixes := crawl.URLPrefixMap{}
	if *onlyPrefixes != "" {
		// If the user gives a list of URLs to allow as prefixes, use those
		for _, u := range crawl.MustParseURLs(strings.Split(*onlyPrefixes, ",")) {
			prefixes.Add(u)
		}
	} else {
		// Otherwise use the seeds
		for _, u := range seeds {
			prefixes.Add(u)
		}
	}

	scope := crawl.AND(
		crawl.NewSchemeScope(strings.Split(*validSchemes, ",")),
		crawl.NewDepthScope(*depth),
		crawl.NewURLPrefixScope(prefixes),
		crawl.NewRegexpIgnoreScope(excludes),
	)
	if !*excludeRelated {
		scope = crawl.AND(crawl.OR(scope, crawl.NewIncludeRelatedScope()), crawl.NewRegexpIgnoreScope(excludes))
	}

	w, err := warcWriterFromFlags()
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close() // nolint

	saver, err := newWarcSaveHandler(w)
	if err != nil {
		log.Fatal(err)
	}

	httpClient = crawl.NewHTTPClientWithDNSOverride(dnsMap)

	crawler, err := crawl.NewCrawler(
		*dbPath,
		seeds,
		scope,
		crawl.FetcherFunc(fetch),
		crawl.HandleRetries(crawl.FollowRedirects(crawl.FilterErrors(saver))),
	)
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

	if *inputFile != "" {
		go func() {
			file, err := os.Open(*inputFile)
			if err != nil {
				log.Fatalf("Couldn't open input-file %s: %+v", *inputFile, err)
			}
			defer file.Close()

			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				urlText := scanner.Text()
				parsedURL, err := url.Parse(urlText)
				if err != nil {
					log.Fatalf("Couldn't parse URL %s: %+v", urlText, err)
				}

				// Slow down reading if the crawler is busy
				if crawler.Busy() {
					time.Sleep(500 * time.Millisecond)
				}

				// Break the reading loop if the user signals exit while we're reading still
				if signaled.Load().(bool) {
					break
				}

				err = crawler.Enqueue(crawl.Outlink{URL: parsedURL, Tag: crawl.TagPrimary}, 0)
				if err != nil {
					log.Fatalf("Couldn't enqueue URL %s: %+v", parsedURL, err)
				}
			}

			if err := scanner.Err(); err != nil {
				log.Fatal(err)
			}
		}()
	}

	crawler.Run(*concurrency)
	crawler.Close()

	if signaled.Load().(bool) {
		os.Exit(1)
	}
	if !*keepDb {
		os.RemoveAll(*dbPath) // nolint
	}
}
