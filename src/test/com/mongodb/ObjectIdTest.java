// ObjectIdTest.java

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

import com.mongodb.util.*;

public class ObjectIdTest extends TestCase {
    
    @Test(groups = {"basic"})
    public void testTSM(){

        ObjectId a = new ObjectId( 2667563522304714314L , -1912742877 );
        assertEquals( "4a26c3e2e316052523dcfd8d" , a.toStringMongod() );
        assertEquals( "250516e3e2c3264a8dfddc23" , a.toStringBabble() );
        assertEquals( "4a26c3e2e316052523dcfd8d" , a.toString() );
    }

    @Test(groups = {"basic"})
    public void testRT1(){
        ObjectId a = new ObjectId();
        assertEquals( a.toStringBabble() , (new ObjectId( a.toStringBabble() , true ) ).toStringBabble() );
        assertEquals( a.toStringMongod() , (new ObjectId( a.toStringMongod() , false ) ).toStringMongod() );
        assertEquals( a.toStringMongod() , (new ObjectId( a.toStringMongod() ) ).toStringMongod() );
        assertEquals( a.toString() , (new ObjectId( a.toString() , false ) ).toString() );
    }

    @Test(groups = {"basic"})
    public void testBabbleToMongo(){
        ObjectId a = new ObjectId();
        assertEquals( a.toStringMongod() , ObjectId.babbleToMongod( a.toStringBabble() ) );
    }

    @Test
    public void testBytes(){
        ObjectId a = new ObjectId();
        assertEquals( a , new ObjectId( a.toByteArray() ) );
        
        byte b[] = new byte[12];
        java.util.Random r = new java.util.Random( 17 );
        for ( int i=0; i<b.length; i++ )
            b[i] = (byte)(r.nextInt());
        a = new ObjectId( b );
        assertEquals( a , new ObjectId( a.toByteArray() ) );        
        assertEquals( "41d91c58988b09375cc1fe9f" , a.toString() );
    }

    public static void main( String args[] )
        throws Exception {
        (new ObjectIdTest()).runConsole();
    }

}
