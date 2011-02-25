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

public class UniqueListTest extends com.mongodb.util.TestCase {

    @org.testng.annotations.Test
    @SuppressWarnings("unchecked")
    public void test1(){
        UniqueList l = new UniqueList();
        l.add( "a" );
        assertEquals( 1 , l.size() );
        l.add( "a" );
        assertEquals( 1 , l.size() );
        l.add( "b" );
        assertEquals( 2 , l.size() );
    }
    
    public static void main( String args[] ){
        (new UniqueListTest()).runConsole();
    }
}
