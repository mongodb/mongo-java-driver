// ThreadingTest.java

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

import com.mongodb.util.TestCase;
import org.testng.annotations.Test;

public class ThreadingTest extends TestCase {
    
    final static String DB = "java_threading_tests";

    public ThreadingTest(){
        cleanupDB = DB;
    }

    @Test
    public void test2Mongos()
        throws Exception {
        Mongo a = new Mongo();
        a.getDB( DB ).getCollection( "test2Mongos" ).insert( new BasicDBObject( "_id" , 1 ) );
        assertEquals( 1 , a.getDB( DB ).getCollection( "test2Mongos" ).findOne().get( "_id" ) );

        Mongo b = new Mongo();
        assertEquals( 1 , b.getDB( DB ).getCollection( "test2Mongos" ).findOne().get( "_id" ) );
        a.close();
        b.close();
    }

    public static void main( String args[] )
        throws Exception {
        (new ThreadingTest()).runConsole();
    }

}
