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

import java.net.*;
import java.util.*;

import org.testng.annotations.Test;

import com.mongodb.util.*;

import org.bson.*;
import org.bson.types.*;

public class DBRefTest extends TestCase {

    public DBRefTest() {
        try {
	    cleanupMongo = new Mongo( "127.0.0.1" );
	    cleanupDB = "com_monogodb_unittest_DBRefTest";
	    _db = cleanupMongo.getDB( cleanupDB );
        }
        catch(UnknownHostException e) {
            throw new MongoException("couldn't connect");
        }
    }

    @Test(groups = {"basic"})
    public void testDBRefBaseToString(){

        ObjectId id = new ObjectId("123456789012345678901234");
        DBRefBase ref = new DBRefBase(_db, "foo.bar", id);

        assertEquals("{ \"$ref\" : \"foo.bar\", \"$id\" : \"123456789012345678901234\" }", ref.toString());
    }

    @Test(groups = {"basic"})
    public void testDBRef(){

        DBRef ref = new DBRef(_db, "hello", (Object)"world");
        DBObject o = new BasicDBObject("!", ref);
        
        OutMessage out = new OutMessage();
        out.putObject( o );
        
        DBCallback cb = new DBCallback( null );
        BSONDecoder decoder = new BSONDecoder();
        decoder.decode( out.toByteArray() , cb );
        DBObject read = cb.dbget();
        
	String correct = null;
	correct = "{\"!\":{\"$ref\":\"hello\",\"$id\":\"world\"}}";

        String got = read.toString().replaceAll( " +" , "" );
        assertEquals( correct , got );
    }

    @Test(groups = {"basic"})
    public void testDBRefFetches(){
        DBCollection coll = _db.getCollection("x");
        coll.drop();
        
        BasicDBObject obj = new BasicDBObject("_id", 321325243);
        coll.save(obj);

        DBRef ref = new DBRef(_db, "x", 321325243);
        DBObject deref = ref.fetch();

        assertTrue(deref != null);
        assertEquals(321325243, ((Number)deref.get("_id")).intValue());

        DBObject refobj = BasicDBObjectBuilder.start().add("$ref", "x").add("$id", 321325243).get();
        deref = DBRef.fetch(_db, refobj);

        assertTrue(deref != null);
        assertEquals(321325243, ((Number)deref.get("_id")).intValue());
    }

    @Test
    public void testRoundTrip(){
        DBCollection a = _db.getCollection( "refroundtripa" );
        DBCollection b = _db.getCollection( "refroundtripb" );
        a.drop();
        b.drop();

        a.save( BasicDBObjectBuilder.start( "_id" , 17 ).add( "n" , 111 ).get() );
        b.save( BasicDBObjectBuilder.start( "n" , 12 ).add( "l" , new DBRef( _db , "refroundtripa" , 17 ) ).get() );
        
        assertEquals( 12 , b.findOne().get( "n" ) );
        assertEquals( DBRef.class , b.findOne().get( "l" ).getClass() );
        assertEquals( 111 , ((DBRef)(b.findOne().get( "l" ))).fetch().get( "n" ) );
        
    }

    DB _db;

    public static void main( String args[] ) {
        (new DBRefTest()).runConsole();
    }
}

