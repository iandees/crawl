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

func (db *gobDB) NewPrefixIterator(ro *levigo.ReadOptions, prefix []byte) *gobIterator {
	i := db.NewIterator(ro)
	i.Seek(prefix)
	return &gobIterator{Iterator: i, prefix: prefix}
}

type gobIterator struct {
	*levigo.Iterator
	prefix []byte
}

func (i *gobIterator) Valid() bool {
	return i.Iterator.Valid() && bytes.HasPrefix(i.Key(), i.prefix)
}

func (i *gobIterator) Value(obj interface{}) error {
	return gob.NewDecoder(bytes.NewBuffer(i.Iterator.Value())).Decode(obj)
}

type URLInfo struct {
	URL        string
	StatusCode int
	CrawledAt  time.Time
	Error      error
}

// A Fetcher retrieves contents from remote URLs.
type Fetcher interface {
	Fetch(string) (*http.Response, error)
}

type FetcherFunc func(string) (*http.Response, error)

func (f FetcherFunc) Fetch(u string) (*http.Response, error) {
	return f(u)
}

// A Handler processes crawled contents. Any errors returned by public
// implementations of this interface are considered permanent and will
// not cause the URL to be fetched again.
type Handler interface {
	Handle(*Crawler, string, int, *http.Response, error) error
}

type HandlerFunc func(*Crawler, string, int, *http.Response, error) error

func (f HandlerFunc) Handle(db *Crawler, u string, depth int, resp *http.Response, err error) error {
	return f(db, u, depth, resp, err)
}

// The Crawler object contains the crawler state.
type Crawler struct {
	db      *gobDB
	seeds   []*url.URL
	scopes  []Scope
	fetcher Fetcher
	handler Handler

	enqueueMx sync.Mutex
}

type queuePair struct {
	Key   []byte
	URL   string
	Depth int
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

	// Create a unique key using the URL and the current timestamp.
	qkey := []byte(fmt.Sprintf("queue/%d/%s", time.Now().Unix(), urlStr))

	// Store the URL in the queue, and store an empty URLInfo to
	// make sure that subsequent calls to Enqueue with the same
	// URL will fail.
	wo := levigo.NewWriteOptions()
	defer wo.Close()
	c.db.PutObj(wo, qkey, &queuePair{Key: qkey, URL: urlStr, Depth: depth})
	c.db.PutObj(wo, ukey, &info)
}

// Scan the queue for URLs until there are no more.
func (c *Crawler) process() <-chan queuePair {
	ch := make(chan queuePair)
	go func() {
		queuePrefix := []byte("queue/")
		for range time.Tick(2 * time.Second) {
			n := 0

			// Scan the queue using a snapshot, to ignore
			// new URLs that might be added after this.
			s := c.db.NewSnapshot()
			ro := levigo.NewReadOptions()
			ro.SetSnapshot(s)

			iter := c.db.NewPrefixIterator(ro, queuePrefix)
			for ; iter.Valid(); iter.Next() {
				var p queuePair
				if err := iter.Value(&p); err != nil {
					continue
				}
				ch <- p
				n++
			}
			iter.Close()

			ro.Close()
			c.db.ReleaseSnapshot(s)

			if n == 0 {
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

		wo := levigo.NewWriteOptions()
		if httpErr == nil {
			respBody.Close()

			// Remove the URL from the queue if the fetcher was successful.
			c.db.Delete(wo, p.Key)
		} else {
			log.Printf("error retrieving %s: %v", p.URL, httpErr)
		}
		c.db.PutObj(wo, urlkey, &info)
		wo.Close()
	}
}

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
		fetcher: f,
		handler: h,
		seeds:   seeds,
		scopes:  scopes,
	}
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
				locationUrl, err := resp.Request.URL.Parse(location)
				if err != nil {
					log.Printf("error parsing Location header: %v", err)
				} else {
					c.Enqueue(locationUrl, depth+1)
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
