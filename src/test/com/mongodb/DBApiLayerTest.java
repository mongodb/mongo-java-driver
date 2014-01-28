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
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DBApiLayerTest extends TestCase {
    private final DBApiLayer db;

    public DBApiLayerTest() throws IOException, MongoException {
        super();
        db = (DBApiLayer) getDatabase();
    }

    @Test
    public void testCursorNotFoundException() {
        DBCollection collection = db.getCollection("testCursorNotFoundException");
        for (int i = 0; i < 150; i++) {
            collection.insert(new BasicDBObject());
        }

        DBCursor cursor = collection.find();
        cursor.next();  // force the query

        db.killCursors(cursor.getServerAddress(), Arrays.asList(cursor.getCursorId()));

        try {
            while (cursor.hasNext()) {
                cursor.next();
            }
            fail("Cursor die!");
        } catch (MongoException.CursorNotFound e) {
            assertEquals(cursor.getServerAddress(), e.getServerAddress());
            assertEquals(cursor.getCursorId(), e.getCursorId());
        } catch (MongoException e) {
            // older mongos implementations incorrectly set the query failure bit
            assertTrue(isSharded(getMongoClient()) && !serverIsAtLeastVersion(2.5));
        }
    }

    @Test(expected =  MongoException.class)
    public void testQueryFailureException() {
        DBCollection collection = db.getCollection("testQueryFailureException");
        collection.insert(new BasicDBObject("loc", new double[]{0, 0}));
        collection.findOne(new BasicDBObject("loc", new BasicDBObject("$near", new double[]{0, 0})));
    }

}
