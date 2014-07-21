/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.codecs;

import com.mongodb.client.DatabaseTestCase;
import com.mongodb.client.MongoCollection;
import org.junit.Test;
import org.mongodb.BSONDocumentBuffer;
import org.mongodb.Document;
import org.mongodb.SimpleBufferProvider;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BSONDocumentBufferCodecTest extends DatabaseTestCase {

    private final BSONDocumentBufferCodec codec =
        new BSONDocumentBufferCodec(new SimpleBufferProvider()
        );

    @Test
    public void shouldBeAbleToQueryThenInsert() {
        List<Document> originalDocuments = new ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            originalDocuments.add(new Document("_id", i).append("b", 2));
        }

        collection.insert(originalDocuments);

        MongoCollection<BSONDocumentBuffer> lazyCollection = database.getCollection(getCollectionName(), codec);
        List<BSONDocumentBuffer> docs = lazyCollection.find().into(new ArrayList<BSONDocumentBuffer>());
        lazyCollection.tools().drop();
        lazyCollection.insert(docs);

        assertEquals(originalDocuments, collection.find().sort(new Document("_id", 1)).into(new ArrayList<Document>()));
    }
}
