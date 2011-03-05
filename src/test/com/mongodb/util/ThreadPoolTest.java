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

package com.mongodb.util;

import org.testng.annotations.Test;

public class ThreadPoolTest extends com.mongodb.util.TestCase {

    @org.testng.annotations.Test
    @SuppressWarnings("unchecked")
    public void test1()
        throws Exception {
        ThreadPool tp = new ThreadPool( "blah" , 2 ){

                public void handle( Object o ){
                    throw new Error();
                }
                
                public void handleError( Object o , Exception e ){
                    System.err.println( "handleError called" );
                }
            };
        
        tp.offer( "silly" );
        Thread.sleep( 20 );

        assertEquals( 0 , tp.inProgress() );
        assertEquals( 0 , tp.numThreads() );
    }

    public static void main( String args[] ){
        (new ThreadPoolTest()).runConsole();
    }
}
