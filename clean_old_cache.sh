#!/bin/bash
# delete cache files old than 1 day

CURDIR=`dirname $0`

echo "Clean old cached files under $CURDIR/cache"

find $CURDIR/cache -type f -mtime +1 | grep -v "\." | xargs rm -f
