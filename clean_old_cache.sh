#!/bin/bash
# delete cache files old than 1 day

CURDIR=$PWD/`dirname $0`

find $CURDIR/cache -type f -mtime 1 | grep -v "\." | xargs rm -f
