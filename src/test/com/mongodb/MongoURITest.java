// MongoURITest.java

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

package com.mongodb;

import java.util.*;
import java.util.regex.*;
import java.io.IOException;

import org.testng.annotations.Test;

import com.mongodb.util.*;

import org.bson.*;
import org.bson.types.*;


public class MongoURITest extends TestCase {

    @Test()
    public void testBasic1(){
        MongoURI u = new MongoURI( "mongodb://foo/bar" );
        assertEquals( 1 , u.getHosts().size() );
        assertEquals( "foo" , u.getHosts().get(0) );
        assertEquals( "bar" , u.getDatabase() );
        assertEquals( null , u.getCollection() );
        assertEquals( null , u.getUsername() );
        assertEquals( null , u.getPassword() );
    }

    @Test()
    public void testBasic2(){
        MongoURI u = new MongoURI( "mongodb://foo/bar.goo" );
        assertEquals( 1 , u.getHosts().size() );
        assertEquals( "foo" , u.getHosts().get(0) );
        assertEquals( "bar" , u.getDatabase() );
        assertEquals( "goo" , u.getCollection() );
    }

    @Test()
    public void testUserPass(){
        MongoURI u = new MongoURI( "mongodb://aaa@bbb:foo/bar" );
        assertEquals( 1 , u.getHosts().size() );
        assertEquals( "foo" , u.getHosts().get(0) );
        assertEquals( "aaa" , u.getUsername() );
        assertEquals( "bbb" , new String( u.getPassword() ) );
    }

    public static void main( String args[] )
        throws Exception {
        (new MongoURITest()).runConsole();

    }

}
