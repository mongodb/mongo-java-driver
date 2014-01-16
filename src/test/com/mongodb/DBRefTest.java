/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import com.mongodb.util.TestCase;
import org.bson.BSONDecoder;
import org.bson.BasicBSONDecoder;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DBRefTest extends TestCase {

    @Test
    public void testEqualsAndHashCode() {
       DBRef ref = new DBRef(getDatabase(), "foo.bar", 4);
       DBRef other = new DBRef(getDatabase(), "foo.bar", 4);
       assertEquals(ref, ref);
       assertEquals(ref, other);
       assertNotEquals(ref, new DBRefBase(getDatabase(), "foo.bar", 4));
       assertEquals(ref.hashCode(), other.hashCode());
    }

    @Test
    public void testDBRefBaseToString(){

        ObjectId id = new ObjectId("123456789012345678901234");
        DBRefBase ref = new DBRefBase(getDatabase(), "foo.bar", id);

        assertEquals("{ \"$ref\" : \"foo.bar\", \"$id\" : \"123456789012345678901234\" }", ref.toString());
    }

    @Test
    public void testDBRef(){

        DBRef ref = new DBRef(getDatabase(), "hello", "world");
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

    @Test
    public void testDBRefFetches(){
        DBCollection coll = collection;
        BasicDBObject obj = new BasicDBObject("_id", 321325243);
        coll.save(obj);

        DBRef ref = new DBRef(getDatabase(), coll.getName(), 321325243);
        DBObject deref = ref.fetch();

        assertTrue(deref != null);
        assertEquals(321325243, ((Number)deref.get("_id")).intValue());

        DBObject refobj = BasicDBObjectBuilder.start().add("$ref", coll.getName()).add("$id", 321325243).get();
        deref = DBRef.fetch(getDatabase(), refobj);

        assertTrue(deref != null);
        assertEquals(321325243, ((Number)deref.get("_id")).intValue());
    }

    @SuppressWarnings("unchecked")
	@Test
    public void testRefListRoundTrip(){
        DBCollection a = collection;
        List<DBRef> refs = new ArrayList<DBRef>();
        refs.add(new DBRef(getDatabase(), "other", 12));
        refs.add(new DBRef(getDatabase(), "other", 14));
        refs.add(new DBRef(getDatabase(), "other", 16));
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
        DBCollection a = getDatabase().getCollection( collection.getName() + ".refroundtripa" );
        DBCollection b = getDatabase().getCollection( collection.getName() + ".refroundtripb" );
        a.drop();
        b.drop();

        a.save( BasicDBObjectBuilder.start( "_id" , 17 ).add( "n" , 111 ).get() );
        b.save( BasicDBObjectBuilder.start( "n" , 12 ).add( "l" , new DBRef( getDatabase() , a.getName() , 17 ) ).get() );

        assertEquals( 12 , b.findOne().get( "n" ) );
        assertEquals( DBRef.class , b.findOne().get( "l" ).getClass() );
        assertEquals( 111 , ((DBRef)(b.findOne().get( "l" ))).fetch().get( "n" ) );

    }

    @Test
    public void testFindByDBRef(){
        DBCollection b = getDatabase().getCollection( "b" );
        b.drop();
        DBRef ref = new DBRef( getDatabase() , "fake" , 17 );

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
        DBCollection a = getDatabase().getCollection("a");
        a.drop();

        BasicDBObject compoundId = new BasicDBObject("name", "someName").append("email", "test@example.com");
        BasicDBObject entity = new BasicDBObject("_id", "testId").append("ref", new DBRef(getDatabase(), "fake", compoundId));
        a.save(entity);

        DBObject fetched = a.findOne(new BasicDBObject("_id", "testId"));

        assertNotNull(fetched);
        assertFalse(fetched.containsField("$id"));
        assertEquals(fetched, entity);
    }

    @Test
    public void testGetEntityWithArrayOfDBRefsWithCompoundIds() {
        DBCollection a = getDatabase().getCollection("a");
        a.drop();

        BasicDBObject compoundId1 = new BasicDBObject("name", "someName").append("email", "test@example.com");
        BasicDBObject compoundId2 = new BasicDBObject("name", "someName2").append("email", "test2@example.com");
        BasicDBList listOfRefs = new BasicDBList();
        listOfRefs.add(new DBRef(getDatabase(), "fake", compoundId1));
        listOfRefs.add(new DBRef(getDatabase(), "fake", compoundId2));
        BasicDBObject entity = new BasicDBObject("_id", "testId").append("refs", listOfRefs);
        a.save(entity);

        DBObject fetched = a.findOne(new BasicDBObject("_id", "testId"));

        assertNotNull(fetched);
        assertEquals(fetched, entity);
    }

    @Test
    public void testGetEntityWithMapOfDBRefsWithCompoundIds() {
        DBCollection base = getDatabase().getCollection("basecollection");
        base.drop();

        BasicDBObject compoundId1 = new BasicDBObject("name", "someName").append("email", "test@example.com");
        BasicDBObject compoundId2 = new BasicDBObject("name", "someName2").append("email", "test2@example.com");
        BasicDBObject mapOfRefs = new BasicDBObject()
                .append("someName", new DBRef(getDatabase(), "compoundkeys", compoundId1))
                .append("someName2", new DBRef(getDatabase(), "compoundkeys", compoundId2));
        BasicDBObject entity = new BasicDBObject("_id", "testId").append("refs", mapOfRefs);
        base.save(entity);

        DBObject fetched = base.findOne(new BasicDBObject("_id", "testId"));

        assertNotNull(fetched);
        DBObject fetchedRefs = (DBObject) fetched.get("refs");
        assertFalse(fetchedRefs.keySet().contains("$id"));
        assertEquals(fetched, entity);
    }
}

