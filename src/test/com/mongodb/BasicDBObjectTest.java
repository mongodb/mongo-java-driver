// BasicDBObjectTest.java

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

public class BasicDBObjectTest extends TestCase {

    @Test(groups = {"basic"})
    public void testBasic(){
        BasicDBObject a = new BasicDBObject( "x" , 1 );
        BasicDBObject b = new BasicDBObject( "x" , 1 );
        assert( a.equals( b ) );
        
        DBObject x = JSON.parse( "{ 'x' : 1 }" );
        assert( a.equals( x ) );
    }


    @Test(groups = {"basic"})
    public void testBasic2(){
        BasicDBObject a = new BasicDBObject( "x" , 1 );
        DBObject b = BasicDBObjectBuilder.start().append( "x" , 1 ).get();
        assert( a.equals( b ) );
        assert( a.equals( JSON.parse( "{ 'x' : 1 }" ) ) );
        assert( ! a.equals( JSON.parse( "{ 'x' : 2 }" ) ) );
    }

    @Test(groups = {"basic"})
    public void testDown1(){
        BasicDBObjectBuilder b = BasicDBObjectBuilder.start();
        b.append( "x" , 1 );
        b.push("y");
        b.append( "a" , 2 );
        b.pop();
        b.push( "z" );
        b.append( "b" , 3 );
        

        DBObject x = b.get();
        DBObject y = JSON.parse( "{ 'x' : 1 , 'y' : { 'a' : 2 } , 'z' : { 'b' : 3 } }" );

        assert( x.equals( y ) );
    }
    
    
    public static void main( String args[] )
        throws Exception {
        (new BasicDBObjectTest()).runConsole();

    }

}
