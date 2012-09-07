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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
    public void testBasic1() throws InterruptedException {
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
    public void testMax1() throws InterruptedException {
	MyPool p = new MyPool( 10 );
	
	int a = p.get();
	assertEquals( 0 , a );
	
	int b = p.get();
	assertEquals( 1 , b );

        // TODO: Fix this test
//	assertNull( p.get( 0 ) );
    }
    
    @org.testng.annotations.Test
    public void testMax2() throws InterruptedException {
	MyPool p = new MyPool( 10 );
	
	int a = p.get();
	assertEquals( 0 , a );
	
	int b = p.get();
	assertEquals( 1 , b );
	
	assertEquals( 2 , (int)p.get( -1 ) );
    }

    @org.testng.annotations.Test
    public void testMax3() throws InterruptedException {
	MyPool p = new MyPool( 10  );
	
	int a = p.get();
	assertEquals( 0 , a );
	
	int b = p.get();
	assertEquals( 1 , b );
	
	assertEquals( 2 , (int)p.get( 1 ) );
    }

    @org.testng.annotations.Test
    public void testThrowErrorFromCreate() throws InterruptedException {
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
    public void testCouldCreate() throws InterruptedException {
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
    public void testReturnNullFromCreate() throws InterruptedException {
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

    @org.testng.annotations.Test()
    public void testThrowsInterruptedException() throws InterruptedException {
            final MyPool p = new MyPool(1);
        try {
            p.get();
        } catch (InterruptedException e) {
            fail("Should not throw InterruptedException here");
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();
        final CountDownLatch ready = new CountDownLatch(1);

        Callable<Boolean> callable = new Callable<Boolean>() {
            @Override
            public Boolean call() {
                try {
                    ready.countDown();
                    p.get();
                    return false;
                } catch (InterruptedException e) {
                    // return true if interrupted
                    return true;
                }
            }
        };
        Future<Boolean> future = executor.submit(callable);

        ready.await();
        // Interrupt the thread
        executor.shutdownNow();

        try {
            assertEquals(true, future.get());
        } catch (InterruptedException e) {
            fail("Should not happen, since this thread was not interrupted");
        } catch (ExecutionException e) {
            fail("Should not happen");
        }
    }

    public static void main( String args[] ){
	SimplePoolTest t = new SimplePoolTest();
	t.runConsole();
    }
}
