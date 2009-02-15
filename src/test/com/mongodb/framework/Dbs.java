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

public class Dbs {

    public static void main(String[] args) 
        throws UnknownHostException {

        Mongo m = new Mongo( new DBAddress( "127.0.0.1:27017/driver_test_framework" ) );
        DBCollection coll = m.getCollection( "dbs_1" );
        DBObject o = new BasicDBObject();
        o.put( "foo", "bar" );
        coll.save( o );

        coll = m.getCollection( "dbs_2" );
        o = new BasicDBObject();
        o.put( "psi", "phi" );
        coll.save( o );

        System.out.println( m.getName() );
        Set<String> set = m.getCollectionNames();
        String[] strs = set.toArray( new String[0] );
        Arrays.sort( strs );
        for( String s : strs ) {
            if( s.startsWith( "dbs" ) ) {
                System.out.println( s );
            }
        }

        m.getCollection( "dbs_1" ).drop();
        o = new BasicDBObject();
        o.put( "create", "dbs_3" );
        m.command( o );

        set = m.getCollectionNames();
        strs = set.toArray( new String[0] );
        Arrays.sort( strs );
        for( String s : strs ) {
            if( s.startsWith( "dbs" ) ) {
                System.out.println( s );
            }
        }
    }
}
