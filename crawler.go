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
	"sync/atomic"
	"time"

	"github.com/PuerkitoBio/purell"
	"github.com/syndtr/goleveldb/leveldb"
	lerr "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	lutil "github.com/syndtr/goleveldb/leveldb/util"
)

var errorRetryDelay = 180 * time.Second

type gobDB struct {
	*leveldb.DB
}

func newGobDB(path string) (*gobDB, error) {
	db, err := leveldb.OpenFile(path, nil)
	if lerr.IsCorrupted(err) {
		log.Printf("corrupted database, recovering...")
		db, err = leveldb.RecoverFile(path, nil)
	}
	if err != nil {
		return nil, err
	}
	return &gobDB{db}, nil
}

func (db *gobDB) PutObjBatch(wb *leveldb.Batch, key []byte, obj interface{}) error {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(obj); err != nil {
		return err
	}
	wb.Put(key, b.Bytes())
	return nil
}

func (db *gobDB) GetObj(key []byte, obj interface{}) error {
	data, err := db.Get(key, nil)
	if err != nil {
		return err
	}
	if err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(obj); err != nil {
		return err
	}
	return nil
}

func (db *gobDB) NewPrefixIterator(prefix []byte) *gobIterator {
	return newGobIterator(db.NewIterator(lutil.BytesPrefix(prefix), nil))
}

func (db *gobDB) NewRangeIterator(startKey, endKey []byte) *gobIterator {
	return newGobIterator(db.NewIterator(&lutil.Range{Start: startKey, Limit: endKey}, nil))
}

type gobIterator struct {
	iterator.Iterator
}

func newGobIterator(i iterator.Iterator) *gobIterator {
	return &gobIterator{i}
}

func (i *gobIterator) Value(obj interface{}) error {
	return gob.NewDecoder(bytes.NewBuffer(i.Iterator.Value())).Decode(obj)
}

// Outlink is a tagged outbound link.
type Outlink struct {
	URL *url.URL
	Tag int
}

const (
	// TagPrimary is a primary reference (another web page).
	TagPrimary = iota

	// TagRelated is a secondary resource, related to a page.
	TagRelated
)

// URLInfo stores information about a crawled URL.
type URLInfo struct {
	URL        string
	StatusCode int
	CrawledAt  time.Time
	Error      string
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
	scope   Scope
	fetcher Fetcher
	handler Handler

	stopCh   chan bool
	stopping atomic.Value

	enqueueMx sync.Mutex
}

// Enqueue a (possibly new) URL for processing.
func (c *Crawler) Enqueue(link Outlink, depth int) error {
	// See if it's in scope.
	if !c.scope.Check(link, depth) {
		return nil
	}

	// Normalize the URL.
	urlStr := purell.NormalizeURL(link.URL, purell.FlagsSafe|purell.FlagRemoveDotSegments|purell.FlagRemoveDuplicateSlashes|purell.FlagRemoveFragment|purell.FlagRemoveDirectoryIndex|purell.FlagSortQuery)

	// Protect the read-modify-update below with a mutex.
	c.enqueueMx.Lock()
	defer c.enqueueMx.Unlock()

	// Check if we've already seen it.
	var info URLInfo
	ukey := []byte(fmt.Sprintf("url/%s", urlStr))
	if err := c.db.GetObj(ukey, &info); err == nil {
		return nil
	}

	// Store the URL in the queue, and store an empty URLInfo to
	// make sure that subsequent calls to Enqueue with the same
	// URL will fail.
	wb := new(leveldb.Batch)
	if err := c.queue.Add(wb, urlStr, depth, time.Now()); err != nil {
		return err
	}
	if err := c.db.PutObjBatch(wb, ukey, &info); err != nil {
		return err
	}
	return c.db.Write(wb, nil)
}

var scanInterval = 1 * time.Second

// Scan the queue for URLs until there are no more.
func (c *Crawler) process() <-chan queuePair {
	ch := make(chan queuePair, 100)
	go func() {
		t := time.NewTicker(scanInterval)
		defer t.Stop()
		defer close(ch)
		for {
			select {
			case <-t.C:
				if err := c.queue.Scan(ch); err != nil {
					return
				}
			case <-c.stopCh:
				return
			}
		}
	}()
	return ch
}

// Main worker loop.
func (c *Crawler) urlHandler(queue <-chan queuePair) {
	for p := range queue {
		// Stop flag needs to short-circuit the queue (which
		// is buffered), or it's going to take a while before
		// we actually stop.
		if c.stopping.Load().(bool) {
			return
		}

		// Retrieve the URLInfo object from the crawl db.
		// Ignore errors, we can work with an empty object.
		urlkey := []byte(fmt.Sprintf("url/%s", p.URL))
		var info URLInfo
		c.db.GetObj(urlkey, &info) // nolint
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

		// Invoke the handler (even if the fetcher errored
		// out). Errors in handling requests are fatal, crawl
		// will be aborted.
		Must(c.handler.Handle(c, p.URL, p.Depth, httpResp, httpErr))

		// Write the result in our database.
		wb := new(leveldb.Batch)
		if httpErr == nil {
			respBody.Close() // nolint

			// Remove the URL from the queue if the fetcher was successful.
			c.queue.Release(wb, p)
		} else {
			info.Error = httpErr.Error()
			log.Printf("error retrieving %s: %v", p.URL, httpErr)
			Must(c.queue.Retry(wb, p, errorRetryDelay))
		}

		Must(c.db.PutObjBatch(wb, urlkey, &info))
		Must(c.db.Write(wb, nil))
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
func NewCrawler(path string, seeds []*url.URL, scope Scope, f Fetcher, h Handler) (*Crawler, error) {
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
		scope:   scope,
		stopCh:  make(chan bool),
	}
	c.stopping.Store(false)

	// Recover active tasks.
	if err := c.queue.Recover(); err != nil {
		return nil, err
	}

	return c, nil
}

// Run the crawl with the specified number of workers. This function
// does not exit until all work is done (no URLs left in the queue).
func (c *Crawler) Run(concurrency int) {
	// Load initial seeds into the queue.
	for _, u := range c.seeds {
		Must(c.Enqueue(Outlink{URL: u, Tag: TagPrimary}, 0))
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

// Stop a running crawl. This will cause a running Run function to return.
func (c *Crawler) Stop() {
	c.stopping.Store(true)
	close(c.stopCh)
}

// Close the database and release resources associated with the crawler state.
func (c *Crawler) Close() {
	c.db.Close() // nolint
}

type redirectHandler struct {
	h Handler
}

func (wrap *redirectHandler) Handle(c *Crawler, u string, depth int, resp *http.Response, err error) error {
	if err != nil {
		return err
	}

	if resp.StatusCode == 200 {
		err = wrap.h.Handle(c, u, depth, resp, err)
	} else if resp.StatusCode > 300 && resp.StatusCode < 400 {
		location := resp.Header.Get("Location")
		if location != "" {
			locationURL, uerr := resp.Request.URL.Parse(location)
			if uerr != nil {
				log.Printf("error parsing Location header: %v", uerr)
			} else {
				Must(c.Enqueue(Outlink{URL: locationURL, Tag: TagPrimary}, depth+1))
			}
		}
	} else {
		err = errors.New(resp.Status)
	}
	return err
}

// NewRedirectHandler returns a Handler that follows HTTP redirects,
// and will call the wrapped handler on every request with HTTP status 200.
func NewRedirectHandler(wrap Handler) Handler {
	return &redirectHandler{wrap}
}

// Must will abort the program with a message when we encounter an
// error that we can't recover from.
func Must(err error) {
	if err != nil {
		log.Fatalf("fatal error: %v", err)
	}
}
