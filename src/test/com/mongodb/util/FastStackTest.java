// FastStackTest.java

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

public class FastStackTest extends com.mongodb.util.TestCase {
    
    @org.testng.annotations.Test(groups = {"basic"})
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
