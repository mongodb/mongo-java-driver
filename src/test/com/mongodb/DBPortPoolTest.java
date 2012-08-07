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

import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DBPortPoolTest extends com.mongodb.util.TestCase {

    @Test
    public void testReuse() throws Exception {
        MongoOptions options = new MongoOptions();
        options.connectionsPerHost = 10;
        final DBPortPool pool = new DBPortPool( new ServerAddress( "localhost" ), options );

        // ensure that maximum number of connections are created
        DBPort[] ports = new DBPort[10];
        for (int x = 0; x < options.connectionsPerHost; x++) {
            ports[x] = pool.get();
            pool.done( ports[x] );
            ports[x]._lastThread = 0;
        }

        int numTasks = 40;

        final CountDownLatch ready = new CountDownLatch(numTasks);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(numTasks);

        ExecutorService es = Executors.newFixedThreadPool( numTasks , Executors.defaultThreadFactory() );
        for(int x = 0; x<numTasks; x++) {
            es.submit( new Runnable() {
                @Override
                public void run(){
                    try {
                        ready.countDown();
                        start.await();
                        DBPort p = pool.get();
                        pool.done(p);
                    } catch (InterruptedException e) {
                        // nada
                    } catch (RuntimeException e) {
                        e.printStackTrace();
                    } catch (Error e) {
                        e.printStackTrace();
                    }

                    done.countDown();
                }
            });
        }
        
        ready.await();
        start.countDown();
        done.await();
        es.shutdown();
        es.awaitTermination( Integer.MAX_VALUE, TimeUnit.SECONDS);
        
        for(int x = 2; x<8; x++) {
//            Assert.assertNotSame( 0 , ports[x]._lastThread, x + ":" + ports[x].hashCode());
        }
        
        assertEquals( pool.getMaxSize() , pool.getAvailable() );
    }

    public static void main( String args[] ){
        (new DBPortPoolTest()).runConsole();
    }
}
