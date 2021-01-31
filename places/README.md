# places

This is the data used to turn outage geometries into place names.

It currently combines "county" data from [whosonfirst](https://github.com/whosonfirst-data/whosonfirst-data-admin-ca)
and Halifax urban neighborhood data from the [HRM Neighourhood Map Project](https://wayemason.ca/archives/hrm-map-project/).

Mostly assembled by hand right now, with the help of a few scripts in this directory to extract the whosonfirst data.

Process:

1. Clone the whosonfirst data repo somewhere (it is large)
2. Run build-db.sh while in that directory, this builds files.db
3. Run extract-ns-featurecollection.sh while in that directory, this builds the county level ns-featurecollection.json
4. Use the process below to combine that ns-featurecollection.json with HRM urban neighorhood data to form a more detailed ns-featurecollection.json
5. Place the complete ns-featurecollection.json in this directory and the build will pick it up

# HRM urban neighourhoods

Download from [here](https://www.google.com/maps/d/u/0/viewer?mid=1i580DOnoSamOTwbNQ9hgVcZPLgY&ll=44.69054410576699%2C-63.59328749999999&z=11) (there's a "Download KML" option available from the menu on the header) then unzip to get the doc.kml within.

```shell
# part of gdal
ogr2ogr out.json doc.kml
# change to the whosonfirst property format for name and placetype, delete duplicate Name and Description
jq -S '.features[].properties |= (. + {"wof:placetype": "neighbourhood", "wof:name": .Name} | del(.Name) | del(.Description))' out.json > reduced.json
# combine county ns-featurecollection.json with reduced neighourhood data
jq -c -s '. as $input | .[0].features |= (. + $input[1].features) | .[0]' ns-featurecollection.json reduced.json > ns-featurecollection-complete.json
mv ns-featurecollection-complete.json ns-featurecollection.json
```
