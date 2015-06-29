package crawl

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/PuerkitoBio/purell"
	"github.com/jmhodges/levigo"
)

type gobDB struct {
	*levigo.DB
}

func newGobDB(path string) (*gobDB, error) {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCache(levigo.NewLRUCache(2 << 20))
	opts.SetFilterPolicy(levigo.NewBloomFilter(10))
	db, err := levigo.Open(path, opts)
	if err != nil {
		return nil, err
	}
	return &gobDB{db}, nil
}

func (db *gobDB) PutObj(wo *levigo.WriteOptions, key []byte, obj interface{}) error {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(obj); err != nil {
		return err
	}
	return db.Put(wo, key, b.Bytes())
}

func (db *gobDB) PutObjBatch(wb *levigo.WriteBatch, key []byte, obj interface{}) error {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(obj); err != nil {
		return err
	}
	wb.Put(key, b.Bytes())
	return nil
}

func (db *gobDB) GetObj(ro *levigo.ReadOptions, key []byte, obj interface{}) error {
	data, err := db.Get(ro, key)
	if err != nil {
		return err
	}
	if err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(obj); err != nil {
		return err
	}
	return nil
}

func (db *gobDB) NewPrefixIterator(ro *levigo.ReadOptions, prefix []byte) *gobPrefixIterator {
	i := db.NewIterator(ro)
	i.Seek(prefix)
	return newGobPrefixIterator(i, prefix)
}

func (db *gobDB) NewRangeIterator(ro *levigo.ReadOptions, startKey, endKey []byte) *gobRangeIterator {
	i := db.NewIterator(ro)
	if startKey != nil {
		i.Seek(startKey)
	}
	return newGobRangeIterator(i, endKey)
}

type gobIterator struct {
	*levigo.Iterator
}

func (i *gobIterator) Value(obj interface{}) error {
	return gob.NewDecoder(bytes.NewBuffer(i.Iterator.Value())).Decode(obj)
}

type gobPrefixIterator struct {
	*gobIterator
	prefix []byte
}

func (i *gobPrefixIterator) Valid() bool {
	return i.gobIterator.Valid() && bytes.HasPrefix(i.Key(), i.prefix)
}

func newGobPrefixIterator(i *levigo.Iterator, prefix []byte) *gobPrefixIterator {
	return &gobPrefixIterator{
		gobIterator: &gobIterator{i},
		prefix:      prefix,
	}
}

type gobRangeIterator struct {
	*gobIterator
	endKey []byte
}

func (i *gobRangeIterator) Valid() bool {
	return i.gobIterator.Valid() && (i.endKey == nil || bytes.Compare(i.Key(), i.endKey) < 0)
}

func newGobRangeIterator(i *levigo.Iterator, endKey []byte) *gobRangeIterator {
	return &gobRangeIterator{
		gobIterator: &gobIterator{i},
		endKey:      endKey,
	}
}

// URLInfo stores information about a crawled URL.
type URLInfo struct {
	URL        string
	StatusCode int
	CrawledAt  time.Time
	Error      error
}

// A Fetcher retrieves contents from remote URLs.
type Fetcher interface {
	// Fetch retrieves a URL and returns the response.
	Fetch(string) (*http.Response, error)
}

// FetcherFunc wraps a simple function into the Fetcher interface.
type FetcherFunc func(string) (*http.Response, error)

// Fetch retrieves a URL and returns the response.
func (f FetcherFunc) Fetch(u string) (*http.Response, error) {
	return f(u)
}

// A Handler processes crawled contents. Any errors returned by public
// implementations of this interface are considered permanent and will
// not cause the URL to be fetched again.
type Handler interface {
	// Handle the response from a URL.
	Handle(*Crawler, string, int, *http.Response, error) error
}

// HandlerFunc wraps a function into the Handler interface.
type HandlerFunc func(*Crawler, string, int, *http.Response, error) error

// Handle the response from a URL.
func (f HandlerFunc) Handle(db *Crawler, u string, depth int, resp *http.Response, err error) error {
	return f(db, u, depth, resp, err)
}

// The Crawler object contains the crawler state.
type Crawler struct {
	db      *gobDB
	queue   *queue
	seeds   []*url.URL
	scopes  []Scope
	fetcher Fetcher
	handler Handler

	enqueueMx sync.Mutex
}

// Enqueue a (possibly new) URL for processing.
func (c *Crawler) Enqueue(u *url.URL, depth int) {
	// Normalize the URL.
	urlStr := purell.NormalizeURL(u, purell.FlagsSafe|purell.FlagRemoveDotSegments|purell.FlagRemoveDuplicateSlashes|purell.FlagRemoveFragment|purell.FlagRemoveDirectoryIndex|purell.FlagSortQuery)

	// See if it's in scope. Checks are ANDed.
	for _, sc := range c.scopes {
		if !sc.Check(u, depth) {
			return
		}
	}

	// Protect the read-modify-update below with a mutex.
	c.enqueueMx.Lock()
	defer c.enqueueMx.Unlock()

	// Check if we've already seen it.
	var info URLInfo
	ro := levigo.NewReadOptions()
	defer ro.Close()
	ukey := []byte(fmt.Sprintf("url/%s", urlStr))
	if err := c.db.GetObj(ro, ukey, &info); err == nil {
		return
	}

	// Store the URL in the queue, and store an empty URLInfo to
	// make sure that subsequent calls to Enqueue with the same
	// URL will fail.
	wb := levigo.NewWriteBatch()
	defer wb.Close()
	c.queue.Add(wb, urlStr, depth, time.Now())
	c.db.PutObjBatch(wb, ukey, &info)
	wo := levigo.NewWriteOptions()
	defer wo.Close()
	c.db.Write(wo, wb)
}

// Scan the queue for URLs until there are no more.
func (c *Crawler) process() <-chan queuePair {
	ch := make(chan queuePair)
	go func() {
		for range time.Tick(2 * time.Second) {
			if err := c.queue.Scan(ch); err != nil {
				break
			}
		}
		close(ch)
	}()
	return ch
}

// Main worker loop.
func (c *Crawler) urlHandler(queue <-chan queuePair) {
	for p := range queue {
		// Retrieve the URLInfo object from the crawl db.
		// Ignore errors, we can work with an empty object.
		urlkey := []byte(fmt.Sprintf("url/%s", p.URL))
		var info URLInfo
		ro := levigo.NewReadOptions()
		c.db.GetObj(ro, urlkey, &info)
		info.CrawledAt = time.Now()
		info.URL = p.URL

		// Fetch the URL and handle it. Make sure to Close the
		// response body (even if it gets replaced in the
		// Response object).
		fmt.Printf("%s\n", p.URL)
		httpResp, httpErr := c.fetcher.Fetch(p.URL)
		var respBody io.ReadCloser
		if httpErr == nil {
			respBody = httpResp.Body
			info.StatusCode = httpResp.StatusCode
		}

		// Invoke the handler (even if the fetcher errored out).
		info.Error = c.handler.Handle(c, p.URL, p.Depth, httpResp, httpErr)

		wb := levigo.NewWriteBatch()
		if httpErr == nil {
			respBody.Close()

			// Remove the URL from the queue if the fetcher was successful.
			c.queue.Release(wb, p)
		} else {
			log.Printf("error retrieving %s: %v", p.URL, httpErr)
			c.queue.Retry(wb, p, 300*time.Second)
		}

		c.db.PutObjBatch(wb, urlkey, &info)

		wo := levigo.NewWriteOptions()
		c.db.Write(wo, wb)
		wo.Close()
		wb.Close()
	}
}

// MustParseURLs parses a list of URLs and aborts on failure.
func MustParseURLs(urls []string) []*url.URL {
	// Parse the seed URLs.
	var parsed []*url.URL
	for _, s := range urls {
		u, err := url.Parse(s)
		if err != nil {
			log.Fatalf("error parsing URL \"%s\": %v", s, err)
		}
		parsed = append(parsed, u)
	}
	return parsed
}

// NewCrawler creates a new Crawler object with the specified behavior.
func NewCrawler(path string, seeds []*url.URL, scopes []Scope, f Fetcher, h Handler) (*Crawler, error) {
	// Open the crawl database.
	db, err := newGobDB(path)
	if err != nil {
		return nil, err
	}

	c := &Crawler{
		db:      db,
		queue:   &queue{db: db},
		fetcher: f,
		handler: h,
		seeds:   seeds,
		scopes:  scopes,
	}

	// Recover active tasks.
	c.queue.Recover()

	return c, nil
}

// Run the crawl with the specified number of workers. This function
// does not exit until all work is done (no URLs left in the queue).
func (c *Crawler) Run(concurrency int) {
	// Load initial seeds into the queue.
	for _, u := range c.seeds {
		c.Enqueue(u, 0)
	}

	// Start some runners and wait until they're done.
	var wg sync.WaitGroup
	ch := c.process()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			c.urlHandler(ch)
			wg.Done()
		}()
	}
	wg.Wait()
}

type redirectHandler struct {
	h Handler
}

func (wrap *redirectHandler) Handle(c *Crawler, u string, depth int, resp *http.Response, err error) error {
	if err == nil {
		if resp.StatusCode == 200 {
			err = wrap.h.Handle(c, u, depth, resp, err)
		} else if resp.StatusCode > 300 && resp.StatusCode < 400 {
			location := resp.Header.Get("Location")
			if location != "" {
				locationURL, err := resp.Request.URL.Parse(location)
				if err != nil {
					log.Printf("error parsing Location header: %v", err)
				} else {
					c.Enqueue(locationURL, depth+1)
				}
			}
		} else {
			err = errors.New(resp.Status)
		}
	}
	return err
}

// NewRedirectHandler returns a Handler that follows HTTP redirects,
// and will call the wrapped handler on every request with HTTP status 200.
func NewRedirectHandler(wrap Handler) Handler {
	return &redirectHandler{wrap}
}
