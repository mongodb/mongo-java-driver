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

package org.mongodb.impl;

import category.Async;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.AsyncCountOperation;
import org.mongodb.operation.AsyncInsertOperation;
import org.mongodb.operation.Find;
import org.mongodb.operation.Insert;
import org.mongodb.session.AsyncSession;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mongodb.Fixture.getAsyncSession;
import static org.mongodb.Fixture.getBufferProvider;
import static org.mongodb.WriteConcern.ACKNOWLEDGED;
import static org.mongodb.WriteConcern.UNACKNOWLEDGED;
import static org.mongodb.session.SessionBindingType.Connection;

@Category(Async.class)
public class MongoAsyncBatchInsertTest extends DatabaseTestCase {

    private List<Document> documents;

    @Before
    public void setUp() throws Exception {
        final byte[] hugeByteArray = new byte[1024 * 1024 * 15];

        documents = new ArrayList<Document>();
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));

        super.setUp();
    }

    @Test
    public void testBatchInsert() throws ExecutionException, InterruptedException {
        final Insert<Document> insert = new Insert<Document>(ACKNOWLEDGED, documents);
        getAsyncSession().execute(new AsyncInsertOperation<Document>(collection.getNamespace(),
                                                                     insert,
                                                                     new DocumentCodec(),
                                                                     getBufferProvider())).get();
        assertEquals(documents.size(), collection.find().count());
    }

    // To make the assertion work for unacknowledged writes, have to bind to a single connection
    @Test
    public void testUnacknowledgedBatchInsert() throws ExecutionException, InterruptedException {
        final Insert<Document> insert = new Insert<Document>(UNACKNOWLEDGED, documents);
        final AsyncInsertOperation<Document> asyncInsertOperation = new AsyncInsertOperation<Document>(collection.getNamespace(),
                                                                                                 insert,
                                                                                                 new DocumentCodec(),
                                                                                                 getBufferProvider());
        final AsyncSession asyncSession = getAsyncSession().getBoundSession(asyncInsertOperation, Connection).get();
        try {
            asyncSession.execute(asyncInsertOperation).get();
            long count =  asyncSession.execute(new AsyncCountOperation(new Find(), collection.getNamespace(), new DocumentCodec(),
                    getBufferProvider())).get();
            assertEquals(documents.size(), count);
        } finally {
            asyncSession.close();
        }
    }
}
