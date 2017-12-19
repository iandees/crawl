package crawl

import (
	"crypto/tls"
	"net/http"
	"net/http/cookiejar"
	"time"
)

var defaultClientTimeout = 60 * time.Second

var DefaultClient *http.Client

// DefaultClient returns a http.Client suitable for crawling: does not
// follow redirects, accepts invalid TLS certificates, sets a
// reasonable timeout for requests.
func init() {
	jar, _ := cookiejar.New(nil)
	DefaultClient = &http.Client{
		Timeout: defaultClientTimeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Jar: jar,
	}
}
