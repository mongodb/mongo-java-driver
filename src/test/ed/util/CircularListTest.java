// CircularListTest.java

/**
*    Copyright (C) 2008 10gen Inc.
*  
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*  
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*  
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package ed.util;

import org.testng.annotations.Test;

public class CircularListTest extends ed.TestCase {

    @Test(groups = {"basic"})
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
    
    @Test(groups = {"basic"})
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
