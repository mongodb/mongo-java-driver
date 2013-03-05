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

import com.mongodb.util.TestCase;
import org.bson.BSONDecoder;
import org.bson.BasicBSONDecoder;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.ObjectId;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class DBRefTest extends TestCase {

    public DBRefTest() {
        cleanupDB = "com_monogodb_unittest_DBRefTest";
        _db = cleanupMongo.getDB(cleanupDB);
    }

    @Test(groups = {"basic"})
    public void testEqualsAndHashCode() {
       DBRef ref = new DBRef(_db, "foo.bar", 4);
       DBRef other = new DBRef(_db, "foo.bar", 4);
       assertEquals(ref, ref);
       assertEquals(ref, other);
       assertNotEquals(ref, new DBRefBase(_db, "foo.bar", 4));
       assertEquals(ref.hashCode(), other.hashCode());
    }

    @Test(groups = {"basic"})
    public void testDBRefBaseToString(){

        ObjectId id = new ObjectId("123456789012345678901234");
        DBRefBase ref = new DBRefBase(_db, "foo.bar", id);

        assertEquals("{ \"$ref\" : \"foo.bar\", \"$id\" : \"123456789012345678901234\" }", ref.toString());
    }

    @Test(groups = {"basic"})
    public void testDBRef(){

        DBRef ref = new DBRef(_db, "hello", "world");
        DBObject o = new BasicDBObject("!", ref);

        DBEncoder encoder = DefaultDBEncoder.FACTORY.create();
        OutputBuffer buf = new BasicOutputBuffer();

        encoder.writeObject(buf, o);

        DefaultDBCallback cb = new DefaultDBCallback( null );
        BSONDecoder decoder = new BasicBSONDecoder();
        decoder.decode( buf.toByteArray() , cb );
        DBObject read = cb.dbget();

        assertEquals("{\"!\":{\"$ref\":\"hello\",\"$id\":\"world\"}}", read.toString().replaceAll( " +" , "" ));
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

    @SuppressWarnings("unchecked")
	@Test
    public void testRefListRoundTrip(){
        DBCollection a = _db.getCollection( "reflistfield" );
        List<DBRef> refs = new ArrayList<DBRef>();
        refs.add(new DBRef(_db, "other", 12));
        refs.add(new DBRef(_db, "other", 14));
        refs.add(new DBRef(_db, "other", 16));
        a.save( BasicDBObjectBuilder.start( "refs" , refs).get() );

        DBObject loaded = a.findOne();
        assertNotNull( loaded );
        List<DBRef> refsLoaded = (List<DBRef>) loaded.get("refs");
        assertNotNull( refsLoaded );
        assertEquals(3, refsLoaded.size());
        assertEquals(DBRef.class, refsLoaded.get(0).getClass());
        assertEquals(12, refsLoaded.get(0).getId());
        assertEquals(14, refsLoaded.get(1).getId());
        assertEquals(16, refsLoaded.get(2).getId());

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

    @Test
    public void testFindByDBRef(){
        DBCollection b = _db.getCollection( "b" );
        b.drop();
        DBRef ref = new DBRef( _db , "fake" , 17 );

        b.save( BasicDBObjectBuilder.start( "n" , 12 ).add( "l" , ref ).get() );

        assertEquals( 12 , b.findOne().get( "n" ) );
        assertEquals( DBRef.class , b.findOne().get( "l" ).getClass() );

        DBObject loaded = b.findOne(BasicDBObjectBuilder.start( "l" , ref ).get() );
        assertEquals( 12 , loaded.get( "n" ) );
        assertEquals( DBRef.class , loaded.get( "l" ).getClass() );
        assertEquals( ref.getId(), ((DBRef)loaded.get( "l" )).getId());
        assertEquals( ref.getRef(), ((DBRef)loaded.get( "l" )).getRef());
        assertEquals( ref.getDB(), ((DBRef)loaded.get( "l" )).getDB());
    }

    @Test
    public void testGetEntityWithSingleDBRefWithCompoundId() {
        DBCollection a = _db.getCollection("a");
        a.drop();

        BasicDBObject compoundId = new BasicDBObject("name", "someName").append("email", "test@example.com");
        BasicDBObject entity = new BasicDBObject("_id", "testId").append("ref", new DBRef(_db, "fake", compoundId));
        a.save(entity);

        DBObject fetched = a.findOne(new BasicDBObject("_id", "testId"));

        assertNotNull(fetched);
        assertFalse(fetched.containsField("$id"));
        assertEquals(fetched, entity);
    }

    @Test
    public void testGetEntityWithArrayOfDBRefsWithCompoundIds() {
        DBCollection a = _db.getCollection("a");
        a.drop();

        BasicDBObject compoundId1 = new BasicDBObject("name", "someName").append("email", "test@example.com");
        BasicDBObject compoundId2 = new BasicDBObject("name", "someName2").append("email", "test2@example.com");
        BasicDBList listOfRefs = new BasicDBList();
        listOfRefs.add(new DBRef(_db, "fake", compoundId1));
        listOfRefs.add(new DBRef(_db, "fake", compoundId2));
        BasicDBObject entity = new BasicDBObject("_id", "testId").append("refs", listOfRefs);
        a.save(entity);

        DBObject fetched = a.findOne(new BasicDBObject("_id", "testId"));

        assertNotNull(fetched);
        assertEquals(fetched, entity);
    }

    @Test
    public void testGetEntityWithMapOfDBRefsWithCompoundIds() {
        DBCollection base = _db.getCollection("basecollection");
        base.drop();

        BasicDBObject compoundId1 = new BasicDBObject("name", "someName").append("email", "test@example.com");
        BasicDBObject compoundId2 = new BasicDBObject("name", "someName2").append("email", "test2@example.com");
        BasicDBObject mapOfRefs = new BasicDBObject()
                .append("someName", new DBRef(_db, "compoundkeys", compoundId1))
                .append("someName2", new DBRef(_db, "compoundkeys", compoundId2));
        BasicDBObject entity = new BasicDBObject("_id", "testId").append("refs", mapOfRefs);
        base.save(entity);

        DBObject fetched = base.findOne(new BasicDBObject("_id", "testId"));

        assertNotNull(fetched);
        DBObject fetchedRefs = (DBObject) fetched.get("refs");
        assertFalse(fetchedRefs.keySet().contains("$id"));
        assertEquals(fetched, entity);
    }

    DB _db;

    public static void main( String args[] ) {
        (new DBRefTest()).runConsole();
    }
}

