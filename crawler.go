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
// implementations of this interface are considered fatal and will
// cause the crawl to abort. The URL will be removed from the queue
// unless the handler returns the special error ErrRetryRequest.
type Handler interface {
	// Handle the response from a URL.
	Handle(Publisher, string, int, int, *http.Response, error) error
}

// HandlerFunc wraps a function into the Handler interface.
type HandlerFunc func(Publisher, string, int, int, *http.Response, error) error

// Handle the response from a URL.
func (f HandlerFunc) Handle(p Publisher, u string, tag, depth int, resp *http.Response, err error) error {
	return f(p, u, tag, depth, resp, err)
}

// ErrRetryRequest is returned by a Handler when the request should be
// retried after some time.
var ErrRetryRequest = errors.New("retry_request")

// Publisher is an interface to something with an Enqueue() method to
// add new potential URLs to crawl.
type Publisher interface {
	Enqueue(Outlink, int) error
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

func normalizeURL(u *url.URL) *url.URL {
	urlStr := purell.NormalizeURL(u,
		purell.FlagsSafe|purell.FlagRemoveDotSegments|purell.FlagRemoveDuplicateSlashes|
			purell.FlagRemoveFragment|purell.FlagSortQuery)
	u2, err := url.Parse(urlStr)
	if err != nil {
		// We *really* do not expect an error here.
		panic(err)
	}
	return u2
}

func seenKey(u *url.URL) []byte {
	return []byte(fmt.Sprintf("_seen/%s", u.String()))
}

func (c *Crawler) hasSeen(u *url.URL) bool {
	_, err := c.db.Get(seenKey(u), nil)
	return err == nil
}

func (c *Crawler) setSeen(wb *leveldb.Batch, u *url.URL) {
	wb.Put(seenKey(u), []byte{})
}

// Enqueue a (possibly new) URL for processing.
func (c *Crawler) Enqueue(link Outlink, depth int) error {
	// Normalize the URL. We are going to replace link.URL in-place, to
	// ensure that scope checks are applied to the normalized URL.
	link.URL = normalizeURL(link.URL)

	// See if it's in scope.
	if !c.scope.Check(link, depth) {
		return nil
	}

	// Protect the read-modify-update below with a mutex.
	c.enqueueMx.Lock()
	defer c.enqueueMx.Unlock()

	// Check if we've already seen it.
	if c.hasSeen(link.URL) {
		return nil
	}

	// Store the URL in the queue, and mark it as seen to make
	// sure that subsequent calls to Enqueue with the same URL
	// will fail.
	wb := new(leveldb.Batch)
	if err := c.queue.Add(wb, link.URL.String(), link.Tag, depth, time.Now()); err != nil {
		return err
	}
	c.setSeen(wb, link.URL)
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

		// Fetch the URL and handle it. Make sure to Close the
		// response body (even if it gets replaced in the
		// Response object).
		httpResp, httpErr := c.fetcher.Fetch(p.URL)
		var respBody io.ReadCloser
		if httpErr == nil {
			log.Printf("HTTP %d %s", httpResp.StatusCode, p.URL)
			respBody = httpResp.Body
		}

		// Invoke the handler (even if the fetcher errored
		// out). Errors in handling requests are fatal, crawl
		// will be aborted.
		err := c.handler.Handle(c, p.URL, p.Tag, p.Depth, httpResp, httpErr)
		if httpErr == nil {
			respBody.Close() // nolint
		}

		wb := new(leveldb.Batch)
		switch err {
		case nil:
			c.queue.Release(wb, p)
		case ErrRetryRequest:
			Must(c.queue.Retry(wb, p, errorRetryDelay))
		default:
			log.Panicf("fatal error in handling %s: %v", p.URL, err)
		}

		// Write the result in our database.
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

// Busy returns true if the crawler has items in it's queue.
func (c *Crawler) Busy() bool {
	activeQueueCount := atomic.LoadInt32(&c.queue.numActive)
	return activeQueueCount > 0
}

// FollowRedirects returns a Handler that follows HTTP redirects
// and adds them to the queue for crawling. It will call the wrapped
// handler on all requests regardless.
func FollowRedirects(wrap Handler) Handler {
	return HandlerFunc(func(p Publisher, u string, tag, depth int, resp *http.Response, err error) error {
		if herr := wrap.Handle(p, u, tag, depth, resp, err); herr != nil {
			return herr
		}

		if err != nil {
			return nil
		}

		location := resp.Header.Get("Location")
		if resp.StatusCode >= 300 && resp.StatusCode < 400 && location != "" {
			locationURL, uerr := resp.Request.URL.Parse(location)
			if uerr != nil {
				log.Printf("error parsing Location header: %v", uerr)
			} else {
				return p.Enqueue(Outlink{URL: locationURL, Tag: tag}, depth+1)
			}
		}
		return nil
	})
}

// FilterErrors returns a Handler that forwards only requests with a
// "successful" HTTP status code (anything < 400). When using this
// wrapper, subsequent Handle calls will always have err set to nil.
func FilterErrors(wrap Handler) Handler {
	return HandlerFunc(func(p Publisher, u string, tag, depth int, resp *http.Response, err error) error {
		if err != nil {
			return nil
		}
		if resp.StatusCode >= 400 {
			return nil
		}
		return wrap.Handle(p, u, tag, depth, resp, nil)
	})
}

// HandleRetries returns a Handler that will retry requests on
// temporary errors (all transport-level errors are considered
// temporary, as well as any HTTP status code >= 500).
func HandleRetries(wrap Handler) Handler {
	return HandlerFunc(func(p Publisher, u string, tag, depth int, resp *http.Response, err error) error {
		if err != nil || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			return ErrRetryRequest
		}
		return wrap.Handle(p, u, tag, depth, resp, nil)
	})
}

// Must will abort the program with a message when we encounter an
// error that we can't recover from.
func Must(err error) {
	if err != nil {
		log.Panicf("fatal error: %v", err)
	}
}
