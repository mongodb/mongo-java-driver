// ThreadPoolTest.java

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

public class ThreadPoolTest extends com.mongodb.util.TestCase {

    @org.testng.annotations.Test
    public void test1()
        throws Exception {
        ThreadPool tp = new ThreadPool( "blah" , 2 ){

                public void handle( Object o ){
                    throw new Error();
                }
                
                public void handleError( Object o , Exception e ){
                    System.err.println( "handleError called" );
                }
            };
        
        tp.offer( "silly" );
        Thread.sleep( 20 );

        assertEquals( 0 , tp.inProgress() );
        assertEquals( 0 , tp.numThreads() );
    }

    public static void main( String args[] ){
        (new ThreadPoolTest()).runConsole();
    }
}
