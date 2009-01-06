// FastStackTest.java

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

public class FastStackTest extends ed.TestCase {
    
    @Test(groups = {"basic"})
    public void testBasic(){
        
        SimpleStack<String> s = new SimpleStack<String>();
        s.push( "a" );
        s.push( "b" );
        assertEquals( 2 , s.size() );
        assertEquals( "b" , s.peek() );
        assertEquals( "b" , s.pop() );
        assertEquals( 1 , s.size() );
        assertEquals( "a" , s.peek() );
        assertEquals( "a" , s.pop() );
        assertEquals( 0 , s.size() );

    }

    public static void main( String args[] ){
        (new FastStackTest()).runConsole();
    }
    
}
