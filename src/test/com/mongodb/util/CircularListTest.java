// CircularListTest.java

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

public class CircularListTest extends TestCase {

    @org.testng.annotations.Test(groups = {"basic"})
    public void testBasicNonFifo()
        throws Exception {

        CircularList<String> l = new CircularList<String>( 3 , false );
        l.add( "a" );

        assertEquals( 1 , l.size() );
        assertEquals( "a" , l.get( 0 ) );
        assertEquals( "[a]" , l.toString() );
        
        l.add( "b" );
        assertEquals( 2 , l.size() );
        assertEquals( "a" , l.get( 0 ) );
        assertEquals( "b" , l.get( 1 ) );

        assertEquals( 3 , l.capacity() );
        
        l.add( "c" );
        assertEquals( 3 , l.size() );
        assertEquals( "a" , l.get( 0 ) );
        assertEquals( "b" , l.get( 1 ) );
        assertEquals( "c" , l.get( 2 ) );

        l.add( "d" );
        assertEquals( 3 , l.size() );
        assertEquals( "b" , l.get( 0 ) );
        assertEquals( "c" , l.get( 1 ) );
        assertEquals( "d" , l.get( 2 ) );
     

        l.add( "e" );
        l.add( "f" );
        l.add( "g" );
        l.add( "h" );

        assertEquals( 3 , l.size() );
        assertEquals( "h" , l.get( 2 ) );
        assertEquals( "g" , l.get( 1 ) );
        assertEquals( "f" , l.get( 0 ) );   

        assertEquals( 3 , l.capacity() );
    }
    
    @org.testng.annotations.Test(groups = {"basic"})
    public void testBasicFifo()
        throws Exception {

        CircularList<String> l = new CircularList<String>( 3 , true );
        l.add( "a" );

        assertEquals( 1 , l.size() );
        assertEquals( "a" , l.get( 0 ) );
        assertEquals( "[a]" , l.toString() );
        
        l.add( "b" );
        assertEquals( 2 , l.size() );
        assertEquals( "b" , l.get( 0 ) );
        assertEquals( "a" , l.get( 1 ) );
        
        l.add( "c" );
        assertEquals( 3 , l.size() );
        assertEquals( "c" , l.get( 0 ) );
        assertEquals( "b" , l.get( 1 ) );
        assertEquals( "a" , l.get( 2 ) );

        l.add( "d" );
        assertEquals( 3 , l.size() );
        assertEquals( "d" , l.get( 0 ) );
        assertEquals( "c" , l.get( 1 ) );
        assertEquals( "b" , l.get( 2 ) );
        
        l.add( "e" );
        l.add( "f" );
        l.add( "g" );
        l.add( "h" );

        assertEquals( 3 , l.size() );
        assertEquals( "h" , l.get( 0 ) );
        assertEquals( "g" , l.get( 1 ) );
        assertEquals( "f" , l.get( 2 ) );
    }


    public static void main( String args[] ){
        (new CircularListTest()).runConsole();
    }
    
}
