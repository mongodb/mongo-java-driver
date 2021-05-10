/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.CursorType;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.Sorts;
import com.mongodb.internal.operation.FindOperation;
import com.mongodb.reactivestreams.client.FindPublisher;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import static com.mongodb.reactivestreams.client.MongoClients.getDefaultCodecRegistry;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("deprecation")
public class FindPublisherImplTest extends TestHelper {

    private static final MongoNamespace NAMESPACE = new MongoNamespace("db", "coll");
    private static final Collation COLLATION = Collation.builder().locale("en").build();

    @DisplayName("Should build the expected FindOperation")
    @Test
    void shouldBuildTheExpectedOperation() {
        configureBatchCursor();

        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor()));
        FindPublisher<Document> publisher = new FindPublisherImpl<>(null, createMongoOperationPublisher(executor), new Document());

        FindOperation<Document> expectedOperation = new FindOperation<>(CSOT_FACTORY_NO_TIMEOUT, NAMESPACE,
                getDefaultCodecRegistry().get(Document.class))
                .batchSize(Integer.MAX_VALUE)
                .retryReads(true)
                .filter(new BsonDocument());

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());

        // Should apply settings
        publisher
                .filter(new Document("filter", 1))
                .sort(Sorts.ascending("sort"))
                .projection(new Document("projection", 1))
                .maxTime(99, MILLISECONDS)
                .maxAwaitTime(999, MILLISECONDS)
                .batchSize(100)
                .limit(100)
                .skip(10)
                .cursorType(CursorType.NonTailable)
                .oplogReplay(false)
                .noCursorTimeout(false)
                .partial(false)
                .collation(COLLATION)
                .comment("my comment")
                .hintString("a_1")
                .min(new Document("min", 1))
                .max(new Document("max", 1))
                .returnKey(false)
                .showRecordId(false)
                .allowDiskUse(false);

        expectedOperation = new FindOperation<>(CSOT_FACTORY_MAX_TIME_AND_MAX_AWAIT_TIME, NAMESPACE,
                getDefaultCodecRegistry().get(Document.class))
                .retryReads(true)
                .allowDiskUse(false)
                .batchSize(100)
                .collation(COLLATION)
                .comment("my comment")
                .cursorType(CursorType.NonTailable)
                .filter(new BsonDocument("filter", new BsonInt32(1)))
                .hint(new BsonString("a_1"))
                .limit(100)
                .max(new BsonDocument("max", new BsonInt32(1)))
                .min(new BsonDocument("min", new BsonInt32(1)))
                .projection(new BsonDocument("projection", new BsonInt32(1)))
                .returnKey(false)
                .showRecordId(false)
                .skip(10)
                .slaveOk(false)
                .sort(new BsonDocument("sort", new BsonInt32(1)));

        configureBatchCursor();
        Flux.from(publisher).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
    }

}
