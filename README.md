A very simple crawler
=====================

This tool can crawl a bunch of URLs for HTML content, and save the
results in a nice WARC file. It has little control over its traffic,
save for a limit on concurrent outbound requests. An external tool
like `trickle` can be used to limit bandwidth.

Its main purpose is to quickly and efficiently save websites for
archival purposes.

The *crawl* tool saves its state in a database, so it can be safely
interrupted and restarted without issues.

# Installation

Assuming you have a proper [Go](https://golang.org/) environment setup,
you can install this package by running:

    $ go install git.autistici.org/ale/crawl/cmd/crawl

This should install the *crawl* binary in your $GOPATH/bin directory.

# Usage

Just run *crawl* by passing the URLs of the websites you want to crawl
as arguments on the command line:

    $ crawl http://example.com/

By default, the tool will store the output WARC file and its own
temporary crawl database in the current directory. This can be
controlled with the *--output* and *--state* command-line options.

The crawling scope is controlled with a set of overlapping checks:

* URL scheme must be one of *http* or *https*
* URL must have one of the seeds as a prefix (an eventual *www.*
  prefix is implicitly ignored)
* maximum crawling depth can be controlled with the *--depth* option
* resources related to a page (CSS, JS, etc) will always be fetched,
  even if on external domains, unless the *--exclude-related* option
  is specified

If the program is interrupted, running it again with the same command
line from the same directory will cause it to resume crawling from
where it stopped. At the end of a successful crawl, the temporary
crawl database will be removed (unless you specify the *--keep*
option, for debugging purposes).

It is possible to tell the crawler to exclude URLs matching specific
regex patterns by using the *--exclude* or *--exclude-from-file*
options. These option may be repeated multiple times. The crawler
comes with its own builtin set of URI regular expressions meant to
avoid calendars, admin panels of common CMS applications, and other
well-known pitfalls. This list is sourced from the
[ArchiveBot](https://github.com/ArchiveTeam/ArchiveBot) project.

## Limitations

Like most crawlers, this one has a number of limitations:

* it completely ignores *robots.txt*. You can make such policy
  decisions yourself by turning the robots.txt into a list of patterns
  to be used with *--exclude-file*.
* it does not embed a Javascript engine, so Javascript-rendered
  elements will not be detected.
* CSS parsing is limited (uses regular expressions), so some *url()*
  resources might not be detected.
* it expects reasonably well-formed HTML, so it may fail to extract
  links from particularly broken pages.
* support for \<object\> and \<video\> tags is limited.
