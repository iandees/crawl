package crawl

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/cookiejar"
	"time"
)

var defaultClientTimeout = 60 * time.Second

// DefaultClient points at a shared http.Client suitable for crawling:
// does not follow redirects, accepts invalid TLS certificates, sets a
// reasonable timeout for requests.
var DefaultClient *http.Client

func init() {
	DefaultClient = NewHTTPClient()
}

// NewHTTPClient returns an http.Client suitable for crawling.
func NewHTTPClient() *http.Client {
	jar, _ := cookiejar.New(nil) // nolint
	return &http.Client{
		Timeout: defaultClientTimeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // nolint
			},
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Jar: jar,
	}
}

// NewHTTPClientWithDNSOverride returns an http.Client suitable for
// crawling, with some additional DNS overrides.
func NewHTTPClientWithDNSOverride(dnsMap map[string]string) *http.Client {
	jar, _ := cookiejar.New(nil) // nolint
	dialer := new(net.Dialer)
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			if override, ok := dnsMap[host]; ok {
				addr = net.JoinHostPort(override, port)
			}
			return dialer.DialContext(ctx, network, addr)
		},
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // nolint
		},
	}
	return &http.Client{
		Timeout:   defaultClientTimeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Jar: jar,
	}
}
