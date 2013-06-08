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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.WriteConcern;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.operation.AsyncInsertOperation;
import org.mongodb.operation.Insert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mongodb.Fixture.getAsyncSession;
import static org.mongodb.Fixture.getBufferProvider;

@Category(Async.class)
public class MongoAsyncBatchInsertTest extends DatabaseTestCase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testBatchInsert() throws ExecutionException, InterruptedException {
        byte[] hugeByteArray = new byte[1024 * 1024 * 15];

        List<Document> documents = new ArrayList<Document>();
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));

        final Insert<Document> insert = new Insert<Document>(documents).writeConcern(WriteConcern.ACKNOWLEDGED);
        new AsyncInsertOperation<Document>(collection.getNamespace(), insert, new DocumentCodec(),
                getBufferProvider()).execute(getAsyncSession()).get();
        assertEquals(documents.size(), collection.count());
    }
}
