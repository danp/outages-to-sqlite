# outages-to-sqlite

A very work-in-progress program to convert
[scraped Nova Scotia Power outage data](https://github.com/danp/nspoweroutages)
to a [SQLite](https://sqlite.org) database.

The result is currently visible at https://nsp.datasette.danp.net/outages
(powered by [Datasette](https://datasette.io)).

In short, this program:

1. Reads each commit of https://github.com/danp/nspoweroutages, cloning it in memory by default, can be configured with `-repo-remote <remote>` or `-repo-path <path>`
1. Parses the `data/outages.json` file
1. Uses the `geom.p` value (which every outage seems to have) as a key to determine which outages are new, ongoing, or gone
1. Uses data in the [places](places) directory to map the `geom.p` value to a place (currently at the "county" level)
1. Emits events to a sqlite database, `outages.db` by default but can be specified with `-database-file <path>`

Once the database exists, subsequent runs will fetch the last observed time from the database and
read commits from then on, picking up where it left off.

Everything about this is subject to change!
