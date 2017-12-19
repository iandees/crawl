package crawl

import (
	"net/url"
	"testing"
)

func mustParseURL(s string) *url.URL {
	u, _ := url.Parse(s)
	return u
}

type testScopeEntry struct {
	uri      string
	depth    int
	expected bool
}

func runScopeTest(t *testing.T, sc Scope, testdata []testScopeEntry) {
	for _, td := range testdata {
		uri := mustParseURL(td.uri)
		result := sc.Check(Outlink{URL: uri, Tag: TagPrimary}, td.depth)
		if result != td.expected {
			t.Errorf("Check(%s, %d) -> got %v, want %v", td.uri, td.depth, result, td.expected)
		}
	}
}

func TestDepthScope(t *testing.T) {
	td := []testScopeEntry{
		{"http://example.com", 1, true},
		{"http://example.com", 10, false},
		{"http://example.com", 100, false},
	}
	runScopeTest(t, NewDepthScope(10), td)
}

func TestSchemeScope(t *testing.T) {
	td := []testScopeEntry{
		{"http://example.com", 0, true},
		{"https://example.com", 0, false},
		{"ftp://example.com", 0, false},
	}
	runScopeTest(t, NewSchemeScope([]string{"http"}), td)
}

func TestURLPrefixScope(t *testing.T) {
	td := []testScopeEntry{
		{"http://example1.com", 0, true},
		{"http://example1.com/", 0, true},
		{"http://example1.com/some/path/", 0, true},
		{"http://www.example1.com", 0, true},
		{"http://subdomain.example1.com", 0, false},

		{"http://example2.com", 0, false},
		{"http://example2.com/", 0, false},
		{"http://www.example2.com", 0, false},
		{"http://example2.com/allowed/path/is/ok", 0, true},
		{"http://example2.com/allowed/path", 0, true},
		{"http://example2.com/another/path/is/not/ok", 0, false},
	}
	pfx := make(URLPrefixMap)
	pfx.Add(mustParseURL("http://example1.com"))
	pfx.Add(mustParseURL("http://example2.com/allowed/path/"))
	runScopeTest(t, NewURLPrefixScope(pfx), td)
}
