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

import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.WriteConcern;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.Count;
import org.mongodb.command.CountCommandResult;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public abstract class AbstractMongoConnectorTest extends DatabaseTestCase {

    private MongoPoolableConnector connector;

    protected void setConnector(final MongoPoolableConnector connector) {
        this.connector = connector;
    }

    @Test
    public void testBatchInsert() {
        byte[] hugeByteArray = new byte[1024 * 1024 * 15];

        List<Document> documents = new ArrayList<Document>();
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));

        final MongoInsert<Document> insert = new MongoInsert<Document>(documents).writeConcern(WriteConcern.ACKNOWLEDGED);
        connector.insert(collection.getNamespace(), insert, new DocumentCodec());
        assertEquals(documents.size(), new CountCommandResult(connector.command(database.getName(),
                new Count(new MongoFind(), collectionName), new DocumentCodec())).getCount());
    }
}
