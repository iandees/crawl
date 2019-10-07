package analysis

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func makeResponse(ctype, body string) *http.Response {
	u, _ := url.Parse("https://example.com/")
	r := &http.Response{
		Header: make(http.Header),
		Body:   ioutil.NopCloser(strings.NewReader(body)),
		Request: &http.Request{
			URL: u,
		},
	}
	r.Header.Set("Content-Type", ctype)
	return r
}

type testdata struct {
	ctype         string
	body          string
	expectedLinks []string
}

func (td *testdata) runTestCase() error {
	links, err := GetLinks(makeResponse(td.ctype, td.body))
	if err != nil {
		return fmt.Errorf("GetLinks() error: %v", err)
	}
	var linkStr []string
	for _, l := range links {
		linkStr = append(linkStr, l.URL.String())
	}
	if diff := cmp.Diff(td.expectedLinks, linkStr); diff != "" {
		return fmt.Errorf("unexpected result:\n%s", diff)
	}
	return nil
}

var tests = []testdata{
	{
		"text/html",
		`
<html><body>
<a href="/link1">link</a>
</body></html>
`,
		[]string{
			"https://example.com/link1",
		},
	},
	{
		"text/html",
		`
<html><head><style type="text/css">
body { background: url('/link1'); }
</style></head>
<body></body></html>
`,
		[]string{
			"https://example.com/link1",
		},
	},
}

func TestLinks(t *testing.T) {
	for _, tt := range tests {
		if err := tt.runTestCase(); err != nil {
			t.Error(err)
		}
	}
}
