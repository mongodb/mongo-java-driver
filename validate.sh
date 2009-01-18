#!/bin/bash
#
#  given the path to the root of the BSON/XSON files, runs
#  through the tree and tests every xson file, generating 
#  "woog.bson" and then comparing
#
for f in `find $1  -name "*.xson"  -print`; do
    FNAME="${f##/*/}"
    FILE="${FNAME%.xson}"
    ROOT=`dirname $f`

    echo "Checking $ROOT/$FILE.xson..."
    java -cp build:. com.mongodb.util.XSON --xtob $ROOT/$FILE.xson  woog.bson
    diff woog.bson $ROOT/$FILE.bson
    if [ $? -ne 0 ]; then 
        exit 1; 
    fi
done

