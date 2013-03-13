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
 *
 */

package com.mongodb;

import com.mongodb.util.TestCase;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

public class DBApiLayerTest extends TestCase {
    private final DBApiLayer db;

    public DBApiLayerTest() throws IOException, MongoException {
        super();
        cleanupDB = "com_mongodb_unittest_DBApiLayerTest";
        db = (DBApiLayer) cleanupMongo.getDB( cleanupDB );
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
        }
    }
}
