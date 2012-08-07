/**
 *      Copyright (C) 2008 - 2012 10gen Inc.
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

public class SimplePoolTest extends com.mongodb.util.TestCase {

    class MyPool extends SimplePool<Integer> {

	MyPool( int size ){
	    super( "blah" , size  );
	}

	public Integer createNew(){
            if (_throwError)
                throw new OutOfMemoryError();

            if (_returnNull) {
                return null;
            }

	    return _num++;
	}

	int _num = 0;
        boolean _throwError;
        boolean _returnNull;
    }

    @org.testng.annotations.Test
    public void testBasic1(){
	MyPool p = new MyPool( 10 );
	
	int a = p.get();
	assertEquals( 0 , a );
	
	int b = p.get();
	assertEquals( 1 , b );
	
	p.done( a );
	a = p.get();
	assertEquals( 0 , a );
    }

    @org.testng.annotations.Test
    public void testMax1(){
	MyPool p = new MyPool( 10 );
	
	int a = p.get();
	assertEquals( 0 , a );
	
	int b = p.get();
	assertEquals( 1 , b );

        // TODO: Fix this test
//	assertNull( p.get( 0 ) );
    }
    
    @org.testng.annotations.Test
    public void testMax2(){
	MyPool p = new MyPool( 10 );
	
	int a = p.get();
	assertEquals( 0 , a );
	
	int b = p.get();
	assertEquals( 1 , b );
	
	assertEquals( 2 , (int)p.get( -1 ) );
    }

    @org.testng.annotations.Test
    public void testMax3(){
	MyPool p = new MyPool( 10  );
	
	int a = p.get();
	assertEquals( 0 , a );
	
	int b = p.get();
	assertEquals( 1 , b );
	
	assertEquals( 2 , (int)p.get( 1 ) );
    }

    @org.testng.annotations.Test
    public void testThrowErrorFromCreate(){
        MyPool p = new MyPool( 1 );
        p._throwError = true;

        try {
            p.get();
            fail("Should have thrown");
        } catch (OutOfMemoryError e) {
            // expected
        }

        p._throwError = false;

        // now make sure there is still a permit left
        Integer a = p.get(0);
        assertEquals( Integer.valueOf(0) , a );
    }

    @org.testng.annotations.Test
    public void testCouldCreate() {
        SimplePool<Integer> p = new SimplePool<Integer>("pool", 2) {
            @Override
            protected Integer createNew() {
                return _num++;
            }

            @Override
            protected int pick(int recommended, boolean couldCreate) {
               if (couldCreate) {
                   return -1;
               }
               return recommended;
            }
            int _num = 1;
        };

        Integer one = p.get();
        assertEquals(Integer.valueOf(1), one);
        p.done(one);

        Integer two = p.get();
        assertEquals(Integer.valueOf(2), two);

        one = p.get();
        assertEquals(Integer.valueOf(1), one);

    }

    @org.testng.annotations.Test
    public void testReturnNullFromCreate(){
        MyPool p = new MyPool( 1 );
        p._returnNull = true;

        try {
            p.get();
            fail("Should have thrown");
        } catch (IllegalStateException e) {
            // expected
        }

        p._returnNull = false;

        // now make sure there is still a permit left
        Integer a = p.get(0);
        assertEquals( Integer.valueOf(0) , a );
    }


    public static void main( String args[] ){
	SimplePoolTest t = new SimplePoolTest();
	t.runConsole();
    }
}
