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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import java.net.UnknownHostException;


public class Capped {

    public static void main(String[] args) 
        throws MongoException , UnknownHostException {

        DB db = new MongoClient().getDB( "driver_test_framework" );
        DBObject foo = new BasicDBObject();
        foo.put( "create", "capped1" );
        foo.put( "capped", true );
        foo.put( "size", 500 );
        DBObject dbobj = db.command( foo );
        DBCollection c = db.getCollection( "capped1" );

        DBObject obj1 = new BasicDBObject();
        obj1.put( "x", 1 );
        c.save( obj1 );
        DBObject obj2 = new BasicDBObject();
        obj2.put( "x", 2 );
        c.save( obj2 );

        foo.put( "create", "capped2" );
        foo.put( "size", 1000 );
        db.command( foo );
        String s = "";
        c = db.getCollection( "capped2" );
        for( int i=1; i<= 100; i++ ) {
            DBObject obj = new BasicDBObject();
            obj.put( "dashes", s );
            c.save( obj );
            s = s+"-";
        }
    }
}
