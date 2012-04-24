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

import org.bson.types.*;
import com.mongodb.util.*;

import org.testng.annotations.*;

// Java
import java.util.Date;

public class BasicDBObjectTest extends TestCase {

    @Test(groups = {"basic"})
    public void testGetDate() {
        final Date date = new Date();
        BasicDBObject doc = new BasicDBObject( "foo" , date);
        assert( doc.getDate( "foo" ).equals( date ) );
    }

    @Test(groups = {"basic"})
    public void testGetDateWithDefault() {
        final Date date = new Date();
        BasicDBObject doc = new BasicDBObject( "foo" , date);
        assert( doc.getDate( "foo", new Date() ).equals( date ) );
        assert( doc.getDate( "bar", date ).equals( date ) );
    }

    @Test(groups = {"basic"})
    public void testGetObjectId() {
        final ObjectId objId = ObjectId.get();
        BasicDBObject doc = new BasicDBObject( "foo" , objId);
        assert( doc.getObjectId( "foo" ).equals( objId ) );
    }

    @Test(groups = {"basic"})
    public void testGetObjectIdWithDefault() {
        final ObjectId objId = ObjectId.get();
        BasicDBObject doc = new BasicDBObject( "foo" , objId);
        assert( doc.getObjectId( "foo", ObjectId.get() ).equals( objId ) );
        assert( doc.getObjectId( "bar", objId ).equals( objId ) );
    }

    @Test(groups = {"basic"})
    public void testGetLongWithDefault() {
        final long test = 100;
        BasicDBObject doc = new BasicDBObject( "foo" , test);
        assert( doc.getLong( "foo", 0l ) == test );
        assert( doc.getLong( "bar", 0l ) == 0l );
    }

    @Test(groups = {"basic"})
    public void testGetDoubleWithDefault() {
        BasicDBObject doc = new BasicDBObject( "foo" , Double.MAX_VALUE);
        assert( doc.getDouble( "foo", (double)0 ) == Double.MAX_VALUE);
        assert( doc.getDouble( "bar", Double.MIN_VALUE ) == Double.MIN_VALUE);
    }

    @Test(groups = {"basic"})
    public void testGetStringWithDefault() {
        BasicDBObject doc = new BasicDBObject( "foo" , "badmf");
        assert( doc.getString( "foo", "ID" ).equals("badmf"));
        assert( doc.getString( "bar", "DEFAULT" ).equals("DEFAULT") );
    }

    @Test(groups = {"basic"})
    public void testBasic(){
        BasicDBObject a = new BasicDBObject( "x" , 1 );
        BasicDBObject b = new BasicDBObject( "x" , 1 );
        assert( a.equals( b ) );

        Object x = JSON.parse( "{ 'x' : 1 }" );
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
    public void testBuilderIsEmpty(){
        BasicDBObjectBuilder b = BasicDBObjectBuilder.start();
        assert( b.isEmpty() );
        b.append( "a" , 1 );
        assert( !b.isEmpty() );
        assert( b.get().equals( JSON.parse( "{ 'a' : 1 }" ) ) );
    }

    @Test(groups = {"basic"})
    public void testBuilderNested(){
        BasicDBObjectBuilder b = BasicDBObjectBuilder.start();
        b.add( "a", 1 );
        b.push( "b" ).append( "c", 2 ).pop();
        DBObject a = b.get();
        assert( a.equals( JSON.parse( "{ 'a' : 1, 'b' : { 'c' : 2 } }" ) ) );
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


        Object x = b.get();
        Object y = JSON.parse( "{ 'x' : 1 , 'y' : { 'a' : 2 } , 'z' : { 'b' : 3 } }" );

        assert( x.equals( y ) );
    }

    void _equal( BasicDBObject x , BasicDBObject y ){
        assert( x.equals( y ) );
        assert( y.equals( x ) );
    }

    void _notequal( BasicDBObject x , BasicDBObject y ){
        assert( ! x.equals( y ) );
        assert( ! y.equals( x ) );
    }

    @Test
    public void testEquals(){
        BasicDBObject a = new BasicDBObject();
        BasicDBObject b = new BasicDBObject();


        _equal( a , b );

        a.put( "x" , 1 );
        _notequal( a , b );

        b.put( "x" , 1 );
        _equal( a , b );

        a.removeField( "x" );
        _notequal( a , b );

        b.removeField( "x" );
        _equal( a , b );

        a.put( "x" , null );
        b.put( "x" , 2 );
        _notequal( a , b );

        a.put( "x" , 2 );
        b.put( "x" , null );
        _notequal( a , b );
    }


    public static void main( String args[] )
        throws Exception {
        (new BasicDBObjectTest()).runConsole();

    }

}
