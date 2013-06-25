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

package org.mongodb.codecs;

import org.bson.BSONBinaryWriter;
import org.bson.io.BasicOutputBuffer;
import org.junit.Test;
import org.mongodb.BSONDocumentBuffer;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.MongoCollection;
import org.mongodb.connection.impl.PowerOfTwoBufferPool;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BSONDocumentBufferCodecTest extends DatabaseTestCase {

    private final BSONDocumentBufferCodec codec =
            new BSONDocumentBufferCodec(new PowerOfTwoBufferPool(24), PrimitiveCodecs.createDefault());

    @Test
    public void shouldBeAbleToQueryThenInsert() {
        final List<Document> originalDocuments = new ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            originalDocuments.add(new Document("_id", i).append("b", 2));
        }

        collection.insert(originalDocuments);

        final MongoCollection<BSONDocumentBuffer> lazyCollection = database.getCollection(getCollectionName(), codec);
        final List<BSONDocumentBuffer> docs = lazyCollection.find().into(new ArrayList<BSONDocumentBuffer>());
        lazyCollection.tools().drop();
        lazyCollection.insert(docs);

        assertEquals(originalDocuments, collection.find().sort(new Document("_id", 1)).into(new ArrayList<Document>()));
    }

    @Test
    public void getIdShouldReturnNullForDocumentWithNoId() {
        final Document doc = new Document("a", 1).append("b", new Document("c", 1));
        final BSONBinaryWriter writer = new BSONBinaryWriter(new BasicOutputBuffer(), true);
        final BSONDocumentBuffer documentBuffer;
        try {
            new DocumentCodec(PrimitiveCodecs.createDefault()).encode(writer, doc);
            documentBuffer = new BSONDocumentBuffer(writer.getBuffer().toByteArray());
        } finally {
            writer.close();
        }

        assertNull(codec.getId(documentBuffer));
    }

    @Test
    public void getIdShouldReturnId() {
        final Integer id = 42;
        final Document doc = new Document("a", 1).append("b", new Document("c", 1)).append("_id", id);
        final BSONBinaryWriter writer = new BSONBinaryWriter(new BasicOutputBuffer(), true);
        final BSONDocumentBuffer documentBuffer;
        try {
            new DocumentCodec(PrimitiveCodecs.createDefault()).encode(writer, doc);
            documentBuffer = new BSONDocumentBuffer(writer.getBuffer().toByteArray());
        } finally {
            writer.close();
        }

        assertEquals(id, codec.getId(documentBuffer));
    }
}
