# places

This is the data used to turn outage geometries into place names.

Currently it's only at the "county" level. It would be nice to get more granular.

It currently comes from https://github.com/whosonfirst-data/whosonfirst-data-admin-ca using the included scripts and queries.

Process:

1. Clone the data repo somewhere (it is large)
2. Run build-db.sh while in that directory, this builds files.db
3. Run extract-ns-featurecollection.sh while in that directory, this builds ns-featurecollection.json
4. Place the resulting ns-featurecollection.json in this `places` directory so a build picks it up
