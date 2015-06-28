package crawl

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

type Scope interface {
	Check(*url.URL, int) bool
}

type maxDepthScope struct {
	maxDepth int
}

func (s *maxDepthScope) Check(uri *url.URL, depth int) bool {
	return depth < s.maxDepth
}

// NewDepthScope returns a Scope that will limit crawls to a
// maximum link depth with respect to the crawl seeds.
func NewDepthScope(maxDepth int) Scope {
	return &maxDepthScope{maxDepth}
}

type schemeScope struct {
	allowedSchemes map[string]struct{}
}

func (s *schemeScope) Check(uri *url.URL, depth int) bool {
	_, ok := s.allowedSchemes[uri.Scheme]
	return ok
}

// NewSchemeScope limits the crawl to the specified URL schemes.
func NewSchemeScope(schemes []string) Scope {
	m := make(map[string]struct{})
	for _, s := range schemes {
		m[s] = struct{}{}
	}
	return &schemeScope{m}
}

// A URLPrefixMap makes it easy to check for URL prefixes (even for
// very large lists). The URL scheme is ignored, along with an
// eventual "www." prefix.
type URLPrefixMap map[string]struct{}

func normalizeUrlPrefix(uri *url.URL) string {
	return strings.TrimPrefix(uri.Host, "www.") + strings.TrimSuffix(uri.Path, "/")
}

func (m URLPrefixMap) Add(uri *url.URL) {
	m[normalizeUrlPrefix(uri)] = struct{}{}
}

func (m URLPrefixMap) Contains(uri *url.URL) bool {
	s := strings.TrimPrefix(uri.Host, "www.")
	if _, ok := m[s]; ok {
		return true
	}
	for _, p := range strings.Split(uri.Path, "/") {
		if p == "" {
			continue
		}
		s = fmt.Sprintf("%s/%s", s, p)
		if _, ok := m[s]; ok {
			return true
		}
	}
	return false
}

type urlPrefixScope struct {
	prefixes URLPrefixMap
}

func (s *urlPrefixScope) Check(uri *url.URL, depth int) bool {
	return s.prefixes.Contains(uri)
}

// NewURLPrefixScope returns a Scope that limits the crawl to a set of
// allowed URL prefixes.
func NewURLPrefixScope(prefixes URLPrefixMap) Scope {
	return &urlPrefixScope{prefixes}
}

// NewSeedScope returns a Scope that will only allow crawling the seed
// prefixes.
func NewSeedScope(seeds []*url.URL) Scope {
	pfx := make(URLPrefixMap)
	for _, s := range seeds {
		pfx.Add(s)
	}
	return NewURLPrefixScope(pfx)
}

type regexpIgnoreScope struct {
	ignores []*regexp.Regexp
}

func (s *regexpIgnoreScope) Check(uri *url.URL, depth int) bool {
	uriStr := uri.String()
	for _, i := range s.ignores {
		if i.MatchString(uriStr) {
			return false
		}
	}
	return true
}

func NewRegexpIgnoreScope(ignores []string) Scope {
	if ignores == nil {
		ignores = defaultIgnorePatterns
	}
	r := regexpIgnoreScope{
		ignores: make([]*regexp.Regexp, 0, len(ignores)),
	}
	for _, i := range ignores {
		r.ignores = append(r.ignores, regexp.MustCompile(i))
	}
	return &r
}
