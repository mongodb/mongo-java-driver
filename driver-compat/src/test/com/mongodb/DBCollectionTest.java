/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class DBCollectionTest extends DatabaseTestCase {
    @Test
    public void testInsert() {
        final WriteResult res = collection.insert(new BasicDBObject("_id", 1).append("x", 2));
        assertNotNull(res);
        assertEquals(1L, collection.count());
        assertEquals(new BasicDBObject("_id", 1).append("x", 2), collection.findOne());
    }

    @Test
    public void testInsertDuplicateKeyException() {
        final DBObject doc = new BasicDBObject("_id", 1);
        collection.insert(doc, WriteConcern.ACKNOWLEDGED);
        try {
            collection.insert(doc, WriteConcern.ACKNOWLEDGED);
            fail("should throw DuplicateKey exception");
        } catch (MongoException.DuplicateKey e) {
            assertThat(e.getCode(), is(11000));
        }
    }

    @Test
    public void testSaveDuplicateKeyException() {
        collection.ensureIndex(new BasicDBObject("x", 1), new BasicDBObject("unique", true));
        collection.save(new BasicDBObject("x", 1), WriteConcern.ACKNOWLEDGED);
        try {
            collection.save(new BasicDBObject("x", 1), WriteConcern.ACKNOWLEDGED);
            fail("should throw DuplicateKey exception");
        } catch (MongoException.DuplicateKey e) {
            assertThat(e.getCode(), is(11000));
        }
    }

    @Test
    public void testUpdate() {
        final WriteResult res = collection.update(new BasicDBObject("_id", 1),
                                           new BasicDBObject("$set", new BasicDBObject("x", 2)),
                                           true, false);
        assertNotNull(res);
        assertEquals(1L, collection.count());
        assertEquals(new BasicDBObject("_id", 1).append("x", 2), collection.findOne());
    }

    @Test
    public void testObjectClass() {
        collection.setObjectClass(MyDBObject.class);
        collection.insert(new BasicDBObject("_id", 1));
        final DBObject obj = collection.findOne();
        assertEquals(MyDBObject.class, obj.getClass());
    }

    @Test
    public void testDotsInKeys() {
        try {
            collection.save(new BasicDBObject("x.y", 1));
            fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            // all good
        }

        try {
            collection.save(new BasicDBObject("x", new BasicDBObject("a.b", 1)));
            fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            // all good
        }

        try {
            final Map<String, Integer> map = new HashMap<String, Integer>();
            map.put("a.b", 1);
            collection.save(new BasicDBObject("x", map));
            fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            // all good
        }
    }

    public static class MyDBObject extends BasicDBObject {
        private static final long serialVersionUID = 3352369936048544621L;
    }
}
