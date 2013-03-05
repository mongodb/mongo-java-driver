// ReplPairTest.java

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

package com.mongodb;

import java.util.*;

public class ReplPairTest {

    static class R extends Thread {
        @SuppressWarnings("deprecation")
        R( ServerAddress a ){
            _a = a;
            _mongo = new MongoClient(a);
            _db = _mongo.getDB( "test" );
            _coll = _db.getCollection( "foo" );

            _coll.slaveOk();
        }

        public void run(){
            while ( true ){
                try {
                    Thread.sleep( 500 );
                    _coll.findOne();
                }
                catch ( NullPointerException n ){
                    n.printStackTrace();
                }
                catch ( Exception e ){
                    System.out.println( _a + "\t" + e );
                }
            }
        }

        final ServerAddress _a;
        final Mongo _mongo;
        final DB _db;
        final DBCollection _coll;
    }

    @SuppressWarnings("deprecation")
    public static void main( String args[] )
        throws Exception {

        List<ServerAddress> addrs = new ArrayList<ServerAddress>();
        addrs.add( new ServerAddress( "localhost" , 9998 ) );
        addrs.add( new ServerAddress( "localhost" , 9999 ) );

        Mongo m = new Mongo ( addrs );
        DB db = m.getDB( "test" );
        DBCollection c = db.getCollection( "foo" );
        c.insert( new BasicDBObject( "_id" , 17 ) );
        c.slaveOk();

        for ( ServerAddress a : addrs ){
            new R(a).start();
        }

        while ( true ){
            Thread.sleep( 500 );
            try {
                System.out.println( c.findOne() );
                c.update( new BasicDBObject( "_id" , 17 ) , new BasicDBObject( "$inc" , new BasicDBObject( "x" , 1 ) ) );
            }
            catch ( Exception e ){
                e.printStackTrace();
            }
        }
    }
}
