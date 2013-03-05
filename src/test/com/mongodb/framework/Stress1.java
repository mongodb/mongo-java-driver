/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb.framework;

import java.util.*;
import java.net.*;

import com.mongodb.*;
import com.mongodb.util.*;

public class Stress1 {

    public static void doStuff( DBCollection c, int count ) 
        throws MongoException {
        DBObject obj = new BasicDBObject();
        obj.put( "id", count );
        DBObject x = c.findOne( obj );
        x.put( "subarray", "foo" + count );
        c.save( x );
    }

    public static void main(String[] args) 
        throws UnknownHostException , MongoException {

        DB db = new MongoClient().getDB( "driver_test_framework" );
        DBCollection c = db.getCollection( "stress1" );

        String blah = "lksjhasoh1298alshasoidiohaskjasiouashoasasiugoas" + 
            "lksjhasoh1298alshasoidiohaskjasiouashoasasiugoas" + 
            "lksjhasoh1298alshasoidiohaskjasiouashoasasiugoas" + 
            "lksjhasoh1298alshasoidiohaskjasiouashoasasiugoas" + 
            "lksjhasoh1298alshasoidiohaskjasiouashoasasiugoas" + 
            "lksjhasoh1298alshasoidiohaskjasiouashoasasiugoas";

        for( int i=0; i<50000; i++ ) {
            DBObject foo = new BasicDBObject();
            foo.put( "name", "asdf"+i );
            foo.put( "date", new Date() );
            foo.put( "id", i );
            foo.put( "blah", blah );
            c.save( foo );
        }

        for( int count=0; count<10000; count++ ) {
            doStuff( c, count );
        }

        DBObject idx = new BasicDBObject();
        idx.put( "date", 1 );
        c.ensureIndex( idx );
    }
}
