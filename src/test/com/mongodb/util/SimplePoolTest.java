// SimplePoolTest.java

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
