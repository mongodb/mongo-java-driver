/*
 * Copyright 2008-present MongoDB, Inc.
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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class DBRefTest extends DatabaseTestCase {

    @SuppressWarnings("unchecked")
    @Test
    public void testRefListRoundTrip() {
        DBCollection a = database.getCollection("reflistfield");
        List<DBRef> refs = new ArrayList<>();
        refs.add(new DBRef("other", 12));
        refs.add(new DBRef("other", 14));
        refs.add(new DBRef("other", 16));
        a.save(new BasicDBObject("refs", refs));

        DBObject loaded = a.findOne();
        assertNotNull(loaded);
        List<DBRef> refsLoaded = (List<DBRef>) loaded.get("refs");
        assertNotNull(refsLoaded);
        assertEquals(3, refsLoaded.size());
        assertEquals(DBRef.class, refsLoaded.get(0).getClass());
        assertEquals(12, refsLoaded.get(0).getId());
        assertEquals(14, refsLoaded.get(1).getId());
        assertEquals(16, refsLoaded.get(2).getId());

    }

    @Test
    public void testRoundTrip() {
        DBCollection a = database.getCollection("refroundtripa");
        DBCollection b = database.getCollection("refroundtripb");
        a.drop();
        b.drop();

        a.save(new BasicDBObject("_id", 17).append("n", 111));
        b.save(new BasicDBObject("n", 12).append("l", new DBRef("refroundtripa", 17)));

        assertEquals(12, b.findOne().get("n"));
        assertEquals(DBRef.class, b.findOne().get("l").getClass());
    }

    @Test
    public void testFindByDBRef() {
        DBRef ref = new DBRef("fake", 17);

        collection.save(new BasicDBObject("n", 12).append("l", ref));

        assertEquals(12, collection.findOne().get("n"));
        assertEquals(DBRef.class, collection.findOne().get("l").getClass());

        DBObject loaded = collection.findOne(new BasicDBObject("l", ref));
        assertEquals(12, loaded.get("n"));
        assertEquals(DBRef.class, loaded.get("l").getClass());
        assertEquals(ref.getId(), ((DBRef) loaded.get("l")).getId());
        assertEquals(ref.getCollectionName(), ((DBRef) loaded.get("l")).getCollectionName());
    }

    @Test
    public void testGetEntityWithSingleDBRefWithCompoundId() {
        DBCollection a = database.getCollection("a");
        a.drop();

        BasicDBObject compoundId = new BasicDBObject("name", "someName").append("email", "test@example.com");
        BasicDBObject entity = new BasicDBObject("_id", "testId").append("ref", new DBRef("fake", compoundId));
        a.save(entity);

        DBObject fetched = a.findOne(new BasicDBObject("_id", "testId"));

        assertNotNull(fetched);
        assertFalse(fetched.containsField("$id"));
        assertEquals(fetched, entity);
    }

    @Test
    public void testGetEntityWithArrayOfDBRefsWithCompoundIds() {
        DBCollection a = database.getCollection("a");
        a.drop();

        BasicDBObject compoundId1 = new BasicDBObject("name", "someName").append("email", "test@example.com");
        BasicDBObject compoundId2 = new BasicDBObject("name", "someName2").append("email", "test2@example.com");
        BasicDBList listOfRefs = new BasicDBList();
        listOfRefs.add(new DBRef("fake", compoundId1));
        listOfRefs.add(new DBRef("fake", compoundId2));
        BasicDBObject entity = new BasicDBObject("_id", "testId").append("refs", listOfRefs);
        a.save(entity);

        DBObject fetched = a.findOne(new BasicDBObject("_id", "testId"));

        assertNotNull(fetched);
        assertEquals(fetched, entity);
    }

    @Test
    public void testGetEntityWithMapOfDBRefsWithCompoundIds() {
        DBCollection base = database.getCollection("basecollection");
        base.drop();

        BasicDBObject compoundId1 = new BasicDBObject("name", "someName").append("email", "test@example.com");
        BasicDBObject compoundId2 = new BasicDBObject("name", "someName2").append("email", "test2@example.com");
        BasicDBObject mapOfRefs = new BasicDBObject().append("someName", new DBRef("compoundkeys", compoundId1))
                                                     .append("someName2", new DBRef("compoundkeys", compoundId2));
        BasicDBObject entity = new BasicDBObject("_id", "testId").append("refs", mapOfRefs);
        base.save(entity);

        DBObject fetched = base.findOne(new BasicDBObject("_id", "testId"));

        assertNotNull(fetched);
        DBObject fetchedRefs = (DBObject) fetched.get("refs");
        assertFalse(fetchedRefs.keySet().contains("$id"));
        assertEquals(fetched, entity);
    }
}
