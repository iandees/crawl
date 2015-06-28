#!/usr/bin/python
#
# Parse ArchiveBot ignore regexp patterns and generate a Go source
# file with a global variable including all of them.
#
# Invoke with a single argument, the location of a checked-out copy of
# https://github.com/ArchiveTeam/ArchiveBot/tree/master/db/ignore_patterns.
#

import glob
import json
import os
import sys

archivebot_ignore_path = sys.argv[1]
print 'package crawl\n\nvar defaultIgnorePatterns = []string{'
for fn in glob.glob(os.path.join(archivebot_ignore_path, '*.json')):
    try:
        with open(fn) as fd:
            print '\n\t// %s' % os.path.basename(fn)
            for p in json.load(fd)['patterns']:
                if '\\\\1' in p or '(?!' in p:
                    # RE2 does not support backreferences or other
                    # fancy PCRE constructs. This excludes <10
                    # patterns from the ignore list.
                    continue
                print '\t%s,' % json.dumps(p)
    except Exception, e:
        print >>sys.stderr, 'error in %s: %s' % (fn, e)
print '}'

