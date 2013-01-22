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

package org.mongodb.serialization.serializers;

import org.bson.BSONBinaryWriter;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Document;
import org.junit.Test;
import org.mongodb.BsonDocumentBuffer;
import org.mongodb.MongoClientTestBase;
import org.mongodb.MongoCollection;
import org.mongodb.SortCriteriaDocument;
import org.mongodb.io.PowerOfTwoByteBufferPool;
import org.mongodb.serialization.BsonDocumentBufferSerializer;
import org.mongodb.serialization.PrimitiveSerializers;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BsonDocumentBufferSerializerTest extends MongoClientTestBase {

    BsonDocumentBufferSerializer serializer =
            new BsonDocumentBufferSerializer(new PowerOfTwoByteBufferPool(24), PrimitiveSerializers.createDefault());

    @Test
    public void shouldBeAbleToQueryThenInsert() {
        List<Document> originalDocuments = new ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            originalDocuments.add(new Document("_id", i).append("b", 2));
        }

        getCollection().insert(originalDocuments);

        MongoCollection<BsonDocumentBuffer> lazyCollection = getCollection(serializer);
        List<BsonDocumentBuffer> docs = lazyCollection.into(new ArrayList<BsonDocumentBuffer>());
        lazyCollection.admin().drop();
        lazyCollection.insert(docs);

        assertEquals(originalDocuments, getCollection().sort(new SortCriteriaDocument("_id", 1)).into(new ArrayList<Document>()));
    }

    @Test
    public void getIdShouldReturnNullForDocumentWithNoId() {
        Document doc = new Document("a", 1).append("b", new Document("c", 1));
        BSONBinaryWriter writer = new BSONBinaryWriter(new BasicOutputBuffer());
        new DocumentSerializer(PrimitiveSerializers.createDefault()).serialize(writer, doc);
        BsonDocumentBuffer documentBuffer = new BsonDocumentBuffer(writer.getBuffer().toByteArray());

        assertNull(serializer.getId(documentBuffer));
    }

    @Test
    public void getIdShouldReturnId() {
        Integer id = 42;
        Document doc = new Document("a", 1).append("b", new Document("c", 1)).append("_id", id);
        BSONBinaryWriter writer = new BSONBinaryWriter(new BasicOutputBuffer());
        new DocumentSerializer(PrimitiveSerializers.createDefault()).serialize(writer, doc);
        BsonDocumentBuffer documentBuffer = new BsonDocumentBuffer(writer.getBuffer().toByteArray());

        assertEquals(id, serializer.getId(documentBuffer));
    }
}
