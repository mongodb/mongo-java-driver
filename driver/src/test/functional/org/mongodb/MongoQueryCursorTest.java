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

package org.mongodb;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.command.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.result.CommandResult;

import static org.junit.Assert.assertEquals;

public class MongoQueryCursorTest extends DatabaseTestCase {
    private MongoQueryCursor<Document> cursor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        for (int i = 0; i < 10; i++) {
            collection.insert(new Document("_id", i));
        }
    }

    @After
    public void tearDown() {
        super.tearDown();
        if (cursor != null) {
            cursor.close();
        }
    }

    @Test
    public void testSizesAndNumGetMores() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new MongoFind().batchSize(2),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), Fixture.getMongoConnector());
        assertEquals(0, cursor.getNumGetMores());
        assertEquals(1, cursor.getSizes().size());
        assertEquals(2, (int) cursor.getSizes().get(0));
        cursor.next();
        cursor.next();
        cursor.next();
        assertEquals(1, cursor.getNumGetMores());
        assertEquals(2, cursor.getSizes().size());
        assertEquals(2, (int) cursor.getSizes().get(1));
        cursor.next();
        cursor.next();
        assertEquals(2, cursor.getNumGetMores());
        assertEquals(3, cursor.getSizes().size());
        assertEquals(2, (int) cursor.getSizes().get(2));
    }

    @Test
    @Ignore("Won't work with replica sets or when tests are run in parallel")
    public void shouldKillCursorIfLimitIsReachedOnInitialQuery() {
        int openCursors = getTotalOpenCursors();
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new MongoFind().limit(5),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), Fixture.getMongoConnector());
        assertEquals(openCursors, getTotalOpenCursors());
    }

    @Test
    @Ignore("Won't work with replica sets or when tests are run in parallel")
    public void shouldKillCursorIfLimitIsReachedOnGetMore() {
        int openCursors = getTotalOpenCursors();
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new MongoFind().batchSize(3).limit(5),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), Fixture.getMongoConnector());
        cursor.next();
        cursor.next();
        cursor.next();
        cursor.next();

        assertEquals(openCursors, getTotalOpenCursors());
    }

    private int getTotalOpenCursors() {
        CommandResult commandResult = Fixture.getMongoClient().getDatabase("admin").executeCommand(new MongoCommand(
                new Document("serverStatus", 1)));
        return (Integer) ((Document) commandResult.getResponse().get("cursors")).get("totalOpen");
    }
}
