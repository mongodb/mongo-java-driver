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

package com.mongodb;

import com.mongodb.client.MongoCollectionOptions;
import com.mongodb.client.NewMongoCollection;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.result.ReplaceOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.codecs.DocumentCodecProvider;
import com.mongodb.operation.ReplaceOperation;
import com.mongodb.operation.UpdateOperation;
import com.mongodb.protocol.AcknowledgedWriteResult;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.RootCodecRegistry;
import org.junit.Test;
import org.mongodb.Document;

import java.util.Arrays;

import static com.mongodb.ReadPreference.secondary;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NewMongoCollectionTest {
    MongoNamespace namespace = new MongoNamespace("db", "coll");
    TestOperationExecutor executor;
    NewMongoCollection<Document> collection;
    CodecRegistry registry = new RootCodecRegistry(Arrays.<CodecProvider>asList(new DocumentCodecProvider()));
    MongoCollectionOptions options = MongoCollectionOptions.builder().writeConcern(WriteConcern.JOURNALED)
                                                           .readPreference(secondary())
                                                           .codecRegistry(registry)
                                                           .build();

    @Test
    public void shouldReplace() {
        // given
        executor = new TestOperationExecutor(new AcknowledgedWriteResult(1, false, null));
        collection = new NewMongoCollectionImpl<Document>(namespace, Document.class, options, executor);

        // when
        ReplaceOneResult result = collection.replaceOne(new ReplaceOneModel<Document, Document>(new Document("_id", 1),
                                                                                                new Document("_id", 1).append("color",
                                                                                                                              "blue")));

        // then
        assertTrue(executor.getWriteOperation() instanceof ReplaceOperation);
        assertEquals(0, result.getModifiedCount());
        assertEquals(1, result.getMatchedCount());
        assertEquals(0, result.getModifiedCount());
        assertNull(result.getUpsertedId());
    }

    @Test
    public void shouldUpdate() {
        // given
        executor = new TestOperationExecutor(new AcknowledgedWriteResult(1, false, null));
        collection = new NewMongoCollectionImpl<Document>(namespace, Document.class, options, executor);

        // when
        UpdateResult result = collection.updateOne(new UpdateOneModel<Document, Document>(new Document("_id", 1),
                                                                                          new Document("$set",
                                                                                                       new Document("color", "blue"))));
        // then
        assertTrue(executor.getWriteOperation() instanceof UpdateOperation);
        assertEquals(0, result.getModifiedCount());
        assertEquals(1, result.getMatchedCount());
        assertEquals(0, result.getModifiedCount());
        assertNull(result.getUpsertedId());
    }
}
