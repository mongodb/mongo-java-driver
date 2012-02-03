// ThreadPoolTest.java

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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;


public class DBPortPoolTest extends com.mongodb.util.TestCase {

    @org.testng.annotations.Test
    public void testReuse() throws Exception {
        final DBPortPool pool = new DBPortPool( new ServerAddress( "localhost" ), new MongoOptions() );
        DBPort[] ports = new DBPort[10];
        for(int x = 0; x<10; x++) {
            ports[x] = pool.get();
            pool.done( ports[x] );
            ports[x]._lastThread = 0;
        }
        
        ExecutorService es = Executors.newFixedThreadPool( 20 , Executors.defaultThreadFactory() );
        for(int x = 0; x<20; x++) {
            es.submit( new Runnable() {
                @Override
                public void run(){
                    try { 
                        Thread.sleep( 100 ); } catch ( InterruptedException e ) { e.printStackTrace( System.out );}
                    DBPort p = pool.get();
                    pool.done( p );
                    //System.out.println( "threadId:" + p._lastThread + " , code:" + p.hashCode() );
                }
            });
        }
        
        Thread.sleep( 3000 );
        
        es.shutdown();
        Assert.assertTrue(es.awaitTermination( 1, TimeUnit.SECONDS ));
        
        for(int x = 2; x<8; x++) {
            Assert.assertNotSame( 0 , ports[x]._lastThread, x + ":" + ports[x].hashCode());
        }
        
        assertEquals( 10 , pool.everCreated() );
        assertEquals( 10 , pool.available() );
        
    }

    public static void main( String args[] ){
        (new DBPortPoolTest()).runConsole();
    }
}
