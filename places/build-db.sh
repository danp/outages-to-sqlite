#!/bin/bash

set -x
set -euo pipefail

if [ -e "files.db" ]; then
    echo "files.db exists" >&2
    exit 1
fi

sqlite3 files.db '
  create table files (path text primary key, data json);
'

(
    echo 'begin;'
    find -E data -type f -regex '.*/[[:digit:]]+\.geojson' -print0 | \
	xargs -0 -I{} echo "insert into files (path, data) values ('{}', readfile('{}'));"
    echo 'commit;'
) | sqlite3 files.db
