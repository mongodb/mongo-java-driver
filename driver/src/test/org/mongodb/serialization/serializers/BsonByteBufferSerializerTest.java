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

import org.bson.types.Document;
import org.junit.Test;
import org.mongodb.MongoClientTestBase;
import org.mongodb.MongoCollection;
import org.mongodb.SortCriteriaDocument;
import org.mongodb.serialization.BsonByteBufferSerializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BsonByteBufferSerializerTest extends MongoClientTestBase {
    @Test
    public void shouldBeAbleToQueryThenInsert() {
        List<Document> originalDocuments = new ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            originalDocuments.add(new Document("_id", i).append("b", 2));
        }

        getCollection().insert(originalDocuments);

        MongoCollection<ByteBuffer> lazyCollection = getCollection(new BsonByteBufferSerializer());
        List<ByteBuffer> docs = lazyCollection.into(new ArrayList<ByteBuffer>());
        lazyCollection.admin().drop();
        for (ByteBuffer cur : docs) {
            cur.flip();
        }
        lazyCollection.insert(docs);


        assertEquals(originalDocuments, getCollection().sort(new SortCriteriaDocument("_id", 1)).into(new ArrayList<Document>()));
    }
}
