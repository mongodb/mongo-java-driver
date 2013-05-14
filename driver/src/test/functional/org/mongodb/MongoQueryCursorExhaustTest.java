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

import org.junit.Before;
import org.junit.Test;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.QueryOption;

import java.util.EnumSet;

import static org.junit.Assert.assertEquals;
import static org.mongodb.Fixture.getBinding;
import static org.mongodb.Fixture.getSession;

public class MongoQueryCursorExhaustTest extends DatabaseTestCase {

    @Before
    public void setUp() throws Exception {
        super.setUp();

        for (int i = 0; i < 1000; i++) {
            collection.insert(new Document("_id", i));
        }
    }

    @Test
    public void testExhaustReadAllDocuments() {
        MongoQueryCursor cursor = new MongoQueryCursor<Document>(collection.getNamespace(),
                new MongoFind().addOptions(EnumSet.of(QueryOption.Exhaust)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession());

        int count = 0;
        while (cursor.hasNext()) {
            cursor.next();
            count++;
        }
        assertEquals(1000, count);
    }

    @Test
    public void testExhaustCloseBeforeReadingAllDocuments() {
        SingleConnectionSession singleConnectionSession = new SingleConnectionSession(getSession().getConnection(), getBinding());

        MongoQueryCursor cursor = new MongoQueryCursor<Document>(collection.getNamespace(),
                new MongoFind().addOptions(EnumSet.of(QueryOption.Exhaust)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), singleConnectionSession);

        cursor.next();
        cursor.close();

        cursor = new MongoQueryCursor<Document>(collection.getNamespace(),
                new MongoFind().limit(1).order(new Document("_id", -1)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), singleConnectionSession);
        assertEquals(new Document("_id", 999), cursor.next());
    }
}
