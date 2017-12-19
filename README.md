A very simple crawler
=====================

This tool can crawl a bunch of URLs for HTML content, and save the
results in a nice WARC file. It has little control over its traffic,
save for a limit on concurrent outbound requests. Its main purpose is
to quickly and efficiently save websites for archival purposes.

The *crawl* tool saves its state in a database, so it can be safely
interrupted and restarted without issues.

# Installation

From this source directory (checked out in the correct place in your
GOPATH), run:

    $ go install cmd/crawl

# Usage

Just run *crawl* by passing the URLs of the websites you want to crawl
as arguments on the command line:

    $ crawl http://example.com/

By default, the tool will store the output WARC file and its own
database in the current directory. This can be controlled with the
*--output* and *--state* command-line options.

The crawling scope is controlled with a set of overlapping checks:

* URL scheme must be one of *http* or *https*
* URL must have one of the seeds as a prefix (an eventual *www.*
  prefix is implicitly ignored)
* maximum crawling depth can be controlled with the *--depth* option
* resources related to a page (CSS, JS, etc) will always be fetched,
  even if on external domains, if the *--include-related* option is
  specified

If the program is interrupted, running it again with the same command
line from the same directory will cause it to resume crawling from
where it stopped. At the end of a successful crawl, the database will
be removed (unless you specify the *--keep* option, for debugging
purposes).
