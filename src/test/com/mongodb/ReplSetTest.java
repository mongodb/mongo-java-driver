// ReplSetTest.java

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

public class ReplSetTest {

    static void _sleep()
        throws InterruptedException {
        //Thread.sleep( 500 );
    }

    static class R extends Thread {
        R( ServerAddress a ){
            _a = a;
            _mongo = new Mongo(a);
            _db = _mongo.getDB( "test" );
            _coll = _db.getCollection( "foo" );
            
            _coll.slaveOk();
        }
        
        public void run(){
            while ( true ){
                try {
                    _sleep();
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

    public static void main( String args[] )
        throws Exception {
        
        boolean rs = true;

        List<ServerAddress> addrs = new ArrayList<ServerAddress>();
        if ( rs ){
            addrs.add( new ServerAddress( "localhost" , 27017 ) );
            addrs.add( new ServerAddress( "localhost" , 27018 ) );
            addrs.add( new ServerAddress( "localhost" , 27019 ) );
        }

        Mongo m = rs ? new Mongo( addrs ) : new Mongo();
        DB db = m.getDB( "test" );
        DBCollection c = db.getCollection( "foo" );
        c.insert( new BasicDBObject( "_id" , 17 ) );
        c.slaveOk();

        for ( ServerAddress a : addrs ){
            new R(a).start();
        }
        
        while ( true ){
            _sleep();
            try {
                DBObject x = c.findOne();
                //System.out.println( x );
                c.update( new BasicDBObject( "_id" , 17 ) , new BasicDBObject( "$inc" , new BasicDBObject( "x" , 1 ) ) );
            }
            catch ( Exception e ){
                e.printStackTrace();
            }
        }
    }
}
