#!/bin/bash

set -x
set -euo pipefail

sqlite3 -noheader -ascii -newline '' files.db < query-featurecollection.sql > ns-featurecollection.json
