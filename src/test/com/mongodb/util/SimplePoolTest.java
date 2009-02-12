// SimplePoolTest.java

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

public class SimplePoolTest extends com.mongodb.util.TestCase {

    class MyPool extends SimplePool<Integer> {

	MyPool( int maxToKeep , int maxTotal ){
	    super( "blah" , maxToKeep , maxTotal );
	}

	public Integer createNew(){
	    return _num++;
	}

	int _num = 0;
    }

    @org.testng.annotations.Test
    public void testBasic1(){
	MyPool p = new MyPool( 10 , 10 );
	
	int a = p.get();
	assertEquals( 0 , a );
	
	int b = p.get();
	assertEquals( 1 , b );
	
	p.done( a );
	a = p.get();
	assertEquals( 0 , a );
    }

    @org.testng.annotations.Test
    public void testBasic2(){
	MyPool p = new MyPool( 0 , 0 );
	
	int a = p.get();
	assertEquals( 0 , a );
	
	int b = p.get();
	assertEquals( 1 , b );
	
	p.done( a );
	a = p.get();
	assertEquals( 2 , a );
    }

    @org.testng.annotations.Test
    public void testMax1(){
	MyPool p = new MyPool( 10 , 2 );
	
	int a = p.get();
	assertEquals( 0 , a );
	
	int b = p.get();
	assertEquals( 1 , b );
	
	assertNull( p.get( 0 ) );
    }
    
    @org.testng.annotations.Test
    public void testMax2(){
	MyPool p = new MyPool( 10 , 3 );
	
	int a = p.get();
	assertEquals( 0 , a );
	
	int b = p.get();
	assertEquals( 1 , b );
	
	assertEquals( 2 , (int)p.get( -1 ) );
    }

    @org.testng.annotations.Test
    public void testMax3(){
	MyPool p = new MyPool( 10 , 3 );
	
	int a = p.get();
	assertEquals( 0 , a );
	
	int b = p.get();
	assertEquals( 1 , b );
	
	assertEquals( 2 , (int)p.get( 1 ) );
    }
    

    public static void main( String args[] ){
	SimplePoolTest t = new SimplePoolTest();
	t.runConsole();
    }
}
