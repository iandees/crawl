// Extract links from HTML/CSS content.

package analysis

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"

	"git.autistici.org/ale/crawl"
)

var (
	urlcssRx = regexp.MustCompile(`(?:@import|:).*url\(["']?([^'"\)]+)["']?\)`)

	linkMatches = []struct {
		tag     string
		attr    string
		linkTag int
	}{
		{"a", "href", crawl.TagPrimary},
		{"link", "href", crawl.TagRelated},
		{"img", "src", crawl.TagRelated},
		{"script", "src", crawl.TagRelated},
		{"iframe", "src", crawl.TagRelated},
	}
)

// The unparsed version of an Outlink.
type rawOutlink struct {
	URL string
	Tag int
}

// GetLinks returns all the links found in a document. Currently only
// parses HTML pages and CSS stylesheets.
func GetLinks(resp *http.Response) ([]crawl.Outlink, error) {
	var outlinks []rawOutlink

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
				outlinks = append(outlinks, rawOutlink{URL: val, Tag: lm.linkTag})
			})
		}
	} else if strings.HasPrefix(ctype, "text/css") {
		// Use a simple (and actually quite bad) regular
		// expression to extract "url()" links from CSS.
		if data, err := ioutil.ReadAll(resp.Body); err == nil {
			for _, val := range urlcssRx.FindAllStringSubmatch(string(data), -1) {
				outlinks = append(outlinks, rawOutlink{URL: val[1], Tag: crawl.TagRelated})
			}
		}
	}

	// Parse outbound links relative to the request URI, and
	// return unique results.
	var result []crawl.Outlink
	links := make(map[string]crawl.Outlink)
	for _, l := range outlinks {
		// Skip data: URLs altogether.
		if strings.HasPrefix(l.URL, "data:") {
			continue
		}
		if linkurl, err := resp.Request.URL.Parse(l.URL); err == nil {
			links[linkurl.String()] = crawl.Outlink{
				URL: linkurl,
				Tag: l.Tag,
			}
		}
	}
	for _, l := range links {
		result = append(result, l)
	}
	return result, nil
}
