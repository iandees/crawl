// Extract links from HTML/CSS content.

package analysis

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

var (
	urlcssRx = regexp.MustCompile(`.*:.*url\(["']?([^'"\)]+)["']?\)`)

	linkMatches = []struct {
		tag  string
		attr string
	}{
		{"a", "href"},
		{"link", "href"},
		{"img", "src"},
		{"script", "src"},
	}
)

// GetLinks returns all the links found in a document. Currently only
// parses HTML pages and CSS stylesheets.
func GetLinks(resp *http.Response) ([]*url.URL, error) {
	var outlinks []string

	ctype := resp.Header.Get("Content-Type")
	if strings.HasPrefix(ctype, "text/html") {
		// Use goquery to extract links from the parsed HTML
		// contents (query patterns are described in the
		// linkMatches table).
		doc, err := goquery.NewDocumentFromResponse(resp)
		if err != nil {
			return nil, err
		}

		for _, lm := range linkMatches {
			doc.Find(fmt.Sprintf("%s[%s]", lm.tag, lm.attr)).Each(func(i int, s *goquery.Selection) {
				val, _ := s.Attr(lm.attr)
				outlinks = append(outlinks, val)
			})
		}
	} else if strings.HasPrefix(ctype, "text/css") {
		// Use a simple (and actually quite bad) regular
		// expression to extract "url()" links from CSS.
		if data, err := ioutil.ReadAll(resp.Body); err == nil {
			for _, val := range urlcssRx.FindAllStringSubmatch(string(data), -1) {
				outlinks = append(outlinks, val[1])
			}
		}
	}

	// Parse outbound links relative to the request URI, and
	// return unique results.
	var result []*url.URL
	links := make(map[string]*url.URL)
	for _, val := range outlinks {
		if linkurl, err := resp.Request.URL.Parse(val); err == nil {
			links[linkurl.String()] = linkurl
		}
	}
	for _, u := range links {
		result = append(result, u)
	}
	return result, nil
}
