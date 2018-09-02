package crawl

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// Scope defines the crawling scope.
type Scope interface {
	// Check a URL to see if it's in scope for crawling.
	Check(Outlink, int) bool
}

type maxDepthScope struct {
	maxDepth int
}

func (s *maxDepthScope) Check(_ Outlink, depth int) bool {
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

func (s *schemeScope) Check(link Outlink, depth int) bool {
	_, ok := s.allowedSchemes[link.URL.Scheme]
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

func normalizeURLPrefix(uri *url.URL) string {
	return strings.TrimPrefix(uri.Host, "www.") + strings.TrimSuffix(uri.Path, "/")
}

// Add an URL to the prefix map.
func (m URLPrefixMap) Add(uri *url.URL) {
	m[normalizeURLPrefix(uri)] = struct{}{}
}

// Contains returns true if the given URL matches the prefix map.
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

func (s *urlPrefixScope) Check(link Outlink, depth int) bool {
	return s.prefixes.Contains(link.URL)
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

func (s *regexpIgnoreScope) Check(link Outlink, depth int) bool {
	uriStr := link.URL.String()
	for _, i := range s.ignores {
		if i.MatchString(uriStr) {
			return false
		}
	}
	return true
}

func compileDefaultIgnorePatterns() []*regexp.Regexp {
	out := make([]*regexp.Regexp, 0, len(defaultIgnorePatterns))
	for _, p := range defaultIgnorePatterns {
		out = append(out, regexp.MustCompile(p))
	}
	return out
}

// NewRegexpIgnoreScope returns a Scope that filters out URLs
// according to a list of regular expressions.
func NewRegexpIgnoreScope(ignores []*regexp.Regexp) Scope {
	ignores = append(compileDefaultIgnorePatterns(), ignores...)
	return &regexpIgnoreScope{
		ignores: ignores,
	}
}

// NewIncludeRelatedScope always includes resources with TagRelated.
func NewIncludeRelatedScope() Scope {
	return &includeRelatedScope{}
}

type includeRelatedScope struct{}

func (s *includeRelatedScope) Check(link Outlink, _ int) bool {
	return link.Tag == TagRelated
}

// AND performs a boolean AND.
func AND(elems ...Scope) Scope {
	return &andScope{elems: elems}
}

type andScope struct {
	elems []Scope
}

func (s *andScope) Check(link Outlink, depth int) bool {
	for _, e := range s.elems {
		if !e.Check(link, depth) {
			return false
		}
	}
	return true
}

// OR performs a boolean OR.
func OR(elems ...Scope) Scope {
	return &orScope{elems: elems}
}

type orScope struct {
	elems []Scope
}

func (s *orScope) Check(link Outlink, depth int) bool {
	for _, e := range s.elems {
		if e.Check(link, depth) {
			return true
		}
	}
	return false
}
