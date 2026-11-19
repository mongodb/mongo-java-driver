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


import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.reactivestreams.client.ClientSession;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.TIMEOUT;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_TIMEOUT;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;


public class MongoOperationPublisherTest {

    private static final OperationExecutor OPERATION_EXECUTOR;

    static {
        OPERATION_EXECUTOR = mock(OperationExecutor.class);
        Mockito.lenient().doAnswer(invocation -> OPERATION_EXECUTOR)
                .when(OPERATION_EXECUTOR)
                .withTimeoutSettings(any());
    }
    private static final MongoNamespace MONGO_NAMESPACE = new MongoNamespace("a.b");

    private static final MongoOperationPublisher<Document> DEFAULT_MOP = new MongoOperationPublisher<>(
            MONGO_NAMESPACE, Document.class, MongoClientSettings.getDefaultCodecRegistry(), ReadPreference.primary(),
            ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED, true, true, null, UuidRepresentation.STANDARD,
            null, TIMEOUT_SETTINGS_WITH_TIMEOUT, OPERATION_EXECUTOR);

    @Test
    public void withCodecRegistry() {
        // Cannot do equality test as registries are wrapped
        CodecRegistry codecRegistry = DEFAULT_MOP.withCodecRegistry(CodecRegistries.fromCodecs(new TestHelper.MyLongCodec())).getCodecRegistry();
        assertTrue(codecRegistry.get(Long.class) instanceof TestHelper.MyLongCodec);
    }

    @Test
    public void withDatabase() {
        assertEquals(new MongoNamespace("c._ignored"), DEFAULT_MOP.withDatabase("c").getNamespace());
    }

    @Test
    public void withDocumentClass() {
        assertEquals(DEFAULT_MOP, DEFAULT_MOP.withDocumentClass(Document.class));
        assertEquals(BsonDocument.class, DEFAULT_MOP.withDocumentClass(BsonDocument.class).getDocumentClass());
    }

    @Test
    public void withDatabaseAndDocumentClass() {
        MongoOperationPublisher<BsonDocument> alternative = DEFAULT_MOP.withDatabaseAndDocumentClass("c", BsonDocument.class);
        assertEquals(BsonDocument.class, alternative.getDocumentClass());
        assertEquals(new MongoNamespace("c._ignored"), alternative.getNamespace());
    }

    @Test
    public void withNamespaceAndDocumentClass() {
        assertEquals(DEFAULT_MOP, DEFAULT_MOP.withNamespaceAndDocumentClass(new MongoNamespace("a.b"), Document.class));

        MongoOperationPublisher<BsonDocument> alternative = DEFAULT_MOP.withNamespaceAndDocumentClass(new MongoNamespace("c.d"),
                BsonDocument.class);
        assertEquals(BsonDocument.class, alternative.getDocumentClass());
        assertEquals(new MongoNamespace("c.d"), alternative.getNamespace());
    }


    @Test
    public void withNamespace() {
        assertEquals(DEFAULT_MOP, DEFAULT_MOP.withNamespaceAndDocumentClass(new MongoNamespace("a.b"), Document.class));
        assertEquals(new MongoNamespace("c.d"), DEFAULT_MOP.withNamespace(new MongoNamespace("c.d")).getNamespace());
    }

    @Test
    public void withReadConcern() {
        assertEquals(DEFAULT_MOP, DEFAULT_MOP.withReadConcern(ReadConcern.DEFAULT));
        assertEquals(ReadConcern.AVAILABLE, DEFAULT_MOP.withReadConcern(ReadConcern.AVAILABLE).getReadConcern());
    }

    @Test
    public void withReadPreference() {
        assertEquals(DEFAULT_MOP, DEFAULT_MOP.withReadPreference(ReadPreference.primary()));
        assertEquals(ReadPreference.secondaryPreferred(), DEFAULT_MOP.withReadPreference(ReadPreference.secondaryPreferred())
                .getReadPreference());
    }

    @Test
    public void withTimeout() {
        assertEquals(DEFAULT_MOP, DEFAULT_MOP.withTimeout(TIMEOUT, TimeUnit.SECONDS));
        assertEquals(1000, DEFAULT_MOP.withTimeout(1000, TimeUnit.MILLISECONDS).getTimeoutMS());
        assertThrows(IllegalArgumentException.class, () -> DEFAULT_MOP.withTimeout(500, TimeUnit.NANOSECONDS));
    }

    @Test
    public void withWriteConcern() {
        assertEquals(DEFAULT_MOP, DEFAULT_MOP.withWriteConcern(WriteConcern.ACKNOWLEDGED));
        assertEquals(WriteConcern.MAJORITY, DEFAULT_MOP.withWriteConcern(WriteConcern.MAJORITY).getWriteConcern());
    }

    @ParameterizedTest(name = "{0}, session={1}, maxTimeMS={2}, timeoutMS={3}")
    @MethodSource("indexTimeoutCases")
    void shouldPreserveIndexTimeoutOptions(final String operation, final boolean withSession,
            final long maxTimeMS, final Long timeoutMS) {
        MongoClientSettings.Builder settings = MongoClientSettings.builder();
        if (timeoutMS != null) {
            settings.timeout(timeoutMS, TimeUnit.MILLISECONDS);
        }
        TestOperationExecutor executor = spy(new TestOperationExecutor(singletonList(null)));
        MongoOperationPublisher<Document> publisher = new MongoOperationPublisher<>(
                MONGO_NAMESPACE, Document.class, MongoClientSettings.getDefaultCodecRegistry(), ReadPreference.primary(),
                ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED, true, true, null, UuidRepresentation.STANDARD,
                null, TimeoutSettings.create(settings.build()), executor);
        ClientSession session = withSession ? mock(ClientSession.class) : null;
        BsonDocument keys = BsonDocument.parse("{x: 1}");
        DropIndexOptions dropOptions = new DropIndexOptions().maxTime(maxTimeMS, TimeUnit.MILLISECONDS);
        Publisher<?> result;
        switch (operation) {
            case "createIndexes":
                result = publisher.createIndexes(session, singletonList(new IndexModel(keys)),
                        new CreateIndexOptions().maxTime(maxTimeMS, TimeUnit.MILLISECONDS));
                break;
            case "dropIndexByName":
                result = publisher.dropIndex(session, "x_1", dropOptions);
                break;
            case "dropIndexByKeys":
                result = publisher.dropIndex(session, keys, dropOptions);
                break;
            case "dropIndexes":
                result = publisher.dropIndexes(session, dropOptions);
                break;
            default:
                throw new AssertionError(operation);
        }
        Flux.from(result).blockLast(Duration.ofSeconds(5));
        ArgumentCaptor<TimeoutSettings> timeoutSettings = ArgumentCaptor.forClass(TimeoutSettings.class);
        verify(executor).withTimeoutSettings(timeoutSettings.capture());
        assertEquals(maxTimeMS, timeoutSettings.getValue().getMaxTimeMS());
        assertEquals(timeoutMS, timeoutSettings.getValue().getTimeoutMS());
        assertSame(session, executor.getClientSession());
    }

    private static Stream<Arguments> indexTimeoutCases() {
        return Stream.of("createIndexes", "dropIndexByName", "dropIndexByKeys", "dropIndexes")
                .flatMap(operation -> Stream.of(false, true)
                        .flatMap(withSession -> Stream.of(0L, 10000L)
                                .flatMap(maxTimeMS -> Stream.of(null, 2000L)
                                        .map(timeoutMS -> Arguments.of(operation, withSession, maxTimeMS, timeoutMS)))));
    }

}
