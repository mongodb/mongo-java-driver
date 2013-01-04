/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package org.mongodb;

import org.bson.types.Document;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class MongoCursorTest extends MongoClientTestBase {
    @Before
    public void before() {
        super.before();
        for (int i = 0; i < 10; i++) {
            collection.insert(new Document("_id", i));
        }
    }


    @Test
    public void testNormalLoopWithGetMore() {
        MongoCursor<Document> cursor = collection.sort(new SortCriteriaDocument("_id", 1)).batchSize(2).find();
        try {
            int i = 0;
            while (cursor.hasNext()) {
                Document cur = cursor.next();
                assertEquals(i++, cur.get("_id"));
            }
            assertEquals(10, i);
            assertFalse(cursor.hasNext());
        } finally {
            cursor.close();
        }
    }

    @Test
    public void testNextWithoutHasNextWithGetMore() {
        MongoCursor<Document> cursor = collection.sort(new SortCriteriaDocument("_id", 1)).batchSize(2).find();
        try {
            for (int i = 0; i < 10; i++) {
                Document cur = cursor.next();
                assertEquals(i, cur.get("_id"));
            }
            assertFalse(cursor.hasNext());
            assertFalse(cursor.hasNext());
            try {
                cursor.next();
                fail();
            } catch (NoSuchElementException e) {
                // this is what we want
            }

        } finally {
            cursor.close();
        }
    }

    @Test
    public void testLimit() {
        List<Document> list = new ArrayList<Document>();
        collection.limit(4).into(list);
        assertEquals(4, list.size());
    }

    @Test
    public void testClose() {
        MongoCursor<Document> cursor = collection.find();

        cursor.next();
        cursor.close();
        try {
            cursor.next();
            fail();
        } catch (IllegalStateException e) {
            // all good
        }

        try {
            cursor.hasNext();
            fail();
        } catch (IllegalStateException e) {
            // all good
        }

        cursor.close();
    }
}
