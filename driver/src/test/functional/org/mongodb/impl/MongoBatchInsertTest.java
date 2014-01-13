/*
 * Copyright (c) 2008 MongoDB, Inc.
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

import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.operation.CountOperation;
import org.mongodb.operation.Find;
import org.mongodb.operation.Insert;
import org.mongodb.operation.InsertOperation;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mongodb.Fixture.getBufferProvider;
import static org.mongodb.Fixture.getSession;
import static org.mongodb.WriteConcern.ACKNOWLEDGED;

public class MongoBatchInsertTest extends DatabaseTestCase {

    @Test
    public void testBatchInsert() {
        byte[] hugeByteArray = new byte[1024 * 1024 * 16 - 100];

        List<Document> documents = new ArrayList<Document>();
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));

        Insert<Document> insert = new Insert<Document>(ACKNOWLEDGED, documents);
        new InsertOperation<Document>(collection.getNamespace(), insert, new DocumentCodec(), getBufferProvider(), getSession(),
                                      false).execute();
        assertEquals((long) documents.size(),
                     (long) new CountOperation(collection.getNamespace(), new Find(), new DocumentCodec(),
                                               getBufferProvider(), getSession(), false).execute());
    }

}
