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


public class Count1 {

    public static void main(String[] args) 
        throws Exception {

        DB db = new MongoClient().getDB( "driver_test_framework" );
        System.out.println( db.getCollection( "test1" ).find().count() );
        System.out.println( db.getCollection( "test2" ).find().count() );
        DBCollection coll = db.getCollection( "test3" );

        DBObject foo = new BasicDBObject();
        foo.put( "i", "a" );
        System.out.println( coll.find( foo ).count() );

        foo.put( "i", 3 );
        System.out.println( coll.find( foo ).count() );

        DBObject bar = new BasicDBObject();
        bar.put( "$gte" , 67 );
        foo.put( "i", bar );
        System.out.println( coll.find( foo ).count() );
    }
}
