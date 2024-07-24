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

import com.mongodb.ClientSessionOptions;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import com.mongodb.reactivestreams.client.MongoCluster;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;


public class MongoClusterImplTest extends TestHelper {

    @Mock
    private ClientSession clientSession;

    private final MongoClusterImpl mongoCluster = createMongoCluster();
    private final MongoOperationPublisher<Document> mongoOperationPublisher = mongoCluster.getMongoOperationPublisher();

    @Test
    public void withCodecRegistry() {
        // Cannot do equality test as registries are wrapped
        CodecRegistry codecRegistry = CodecRegistries.fromCodecs(new MyLongCodec());
        MongoCluster newMongoCluster = mongoCluster.withCodecRegistry(codecRegistry);
        assertTrue(newMongoCluster.getCodecRegistry().get(Long.class) instanceof TestHelper.MyLongCodec);
    }

    @Test
    public void withReadConcern() {
        assertEquals(ReadConcern.AVAILABLE, mongoCluster.withReadConcern(ReadConcern.AVAILABLE).getReadConcern());
    }

    @Test
    public void withReadPreference() {
        assertEquals(ReadPreference.secondaryPreferred(), mongoCluster.withReadPreference(ReadPreference.secondaryPreferred())
                .getReadPreference());
    }

    @Test
    public void withTimeout() {
        assertEquals(1000, mongoCluster.withTimeout(1000, TimeUnit.MILLISECONDS).getTimeout(TimeUnit.MILLISECONDS));
    }

    @Test
    public void withWriteConcern() {
        assertEquals(WriteConcern.MAJORITY, mongoCluster.withWriteConcern(WriteConcern.MAJORITY).getWriteConcern());
    }

    @Test
    void testListDatabases() {
        assertAll("listDatabases",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoCluster.listDatabases((Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoCluster.listDatabases((ClientSession) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> mongoCluster.listDatabases(clientSession, null))),
                  () -> {
                      ListDatabasesPublisher<Document> expected =
                              new ListDatabasesPublisherImpl<>(null, mongoOperationPublisher);
                      assertPublisherIsTheSameAs(expected, mongoCluster.listDatabases(), "Default");
                  },
                  () -> {
                      ListDatabasesPublisher<Document> expected =
                              new ListDatabasesPublisherImpl<>(clientSession, mongoOperationPublisher);
                      assertPublisherIsTheSameAs(expected, mongoCluster.listDatabases(clientSession), "With session");
                  },
                  () -> {
                      ListDatabasesPublisher<BsonDocument> expected =
                              new ListDatabasesPublisherImpl<>(null, mongoOperationPublisher
                                      .withDocumentClass(BsonDocument.class));
                      assertPublisherIsTheSameAs(expected, mongoCluster.listDatabases(BsonDocument.class), "Alternative class");
                  },
                  () -> {
                      ListDatabasesPublisher<BsonDocument> expected =
                              new ListDatabasesPublisherImpl<>(clientSession, mongoOperationPublisher
                                      .withDocumentClass(BsonDocument.class));
                      assertPublisherIsTheSameAs(expected, mongoCluster.listDatabases(clientSession, BsonDocument.class),
                                                 "Alternative class with session");
                  }
        );
    }

    @Test
    void testListDatabaseNames() {
        assertAll("listDatabaseNames",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoCluster.listDatabaseNames(null))),
                  () -> {
                      ListDatabasesPublisher<Document> expected =
                              new ListDatabasesPublisherImpl<>(null, mongoOperationPublisher).nameOnly(true);

                      assertPublisherIsTheSameAs(expected, mongoCluster.listDatabaseNames(), "Default");
                  },
                  () -> {
                      ListDatabasesPublisher<Document> expected =
                              new ListDatabasesPublisherImpl<>(clientSession, mongoOperationPublisher).nameOnly(true);

                      assertPublisherIsTheSameAs(expected, mongoCluster.listDatabaseNames(clientSession), "With session");
                  }
        );
    }

    @Test
    void testWatch() {
        List<Bson> pipeline = singletonList(BsonDocument.parse("{$match: {open: true}}"));
        assertAll("watch",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoCluster.watch((Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoCluster.watch((List<Bson>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoCluster.watch(pipeline, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoCluster.watch((ClientSession) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoCluster.watch(null, pipeline)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> mongoCluster.watch(null, pipeline, Document.class))
                  ),
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher.withDatabase("admin"),
                                                              Document.class, emptyList(), ChangeStreamLevel.CLIENT);
                      assertPublisherIsTheSameAs(expected, mongoCluster.watch(), "Default");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher.withDatabase("admin"),
                                                              Document.class, pipeline, ChangeStreamLevel.CLIENT);
                      assertPublisherIsTheSameAs(expected, mongoCluster.watch(pipeline), "With pipeline");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher.withDatabase("admin"),
                                                              BsonDocument.class, emptyList(), ChangeStreamLevel.CLIENT);
                      assertPublisherIsTheSameAs(expected, mongoCluster.watch(BsonDocument.class),
                                                 "With result class");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher.withDatabase("admin"),
                                                              BsonDocument.class, pipeline, ChangeStreamLevel.CLIENT);
                      assertPublisherIsTheSameAs(expected, mongoCluster.watch(pipeline, BsonDocument.class),
                                                 "With pipeline & result class");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              new ChangeStreamPublisherImpl<>(clientSession, mongoOperationPublisher.withDatabase("admin"),
                                                              Document.class, emptyList(), ChangeStreamLevel.CLIENT);
                      assertPublisherIsTheSameAs(expected, mongoCluster.watch(clientSession), "with session");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              new ChangeStreamPublisherImpl<>(clientSession, mongoOperationPublisher.withDatabase("admin"),
                                                              Document.class, pipeline, ChangeStreamLevel.CLIENT);
                      assertPublisherIsTheSameAs(expected, mongoCluster.watch(clientSession, pipeline), "With session & pipeline");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              new ChangeStreamPublisherImpl<>(clientSession, mongoOperationPublisher.withDatabase("admin"),
                                                              BsonDocument.class, emptyList(), ChangeStreamLevel.CLIENT);
                      assertPublisherIsTheSameAs(expected, mongoCluster.watch(clientSession, BsonDocument.class),
                                                 "With session & resultClass");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              new ChangeStreamPublisherImpl<>(clientSession, mongoOperationPublisher.withDatabase("admin"),
                                                              BsonDocument.class, pipeline, ChangeStreamLevel.CLIENT);
                      assertPublisherIsTheSameAs(expected, mongoCluster.watch(clientSession, pipeline, BsonDocument.class),
                                                 "With clientSession, pipeline & result class");
                  }
        );
    }

    @Test
    void testStartSession() {
        MongoClusterImpl mongoCluster = createMongoCluster();

        // Validation
        assertThrows(IllegalArgumentException.class, () -> mongoCluster.startSession(null));

        // Default
        Mono<ClientSession> expected = mongoCluster.getClientSessionHelper()
                .createClientSessionMono(ClientSessionOptions.builder().build(), OPERATION_EXECUTOR);
        assertPublisherIsTheSameAs(expected, mongoCluster.startSession(), "Default");

        // with options
        ClientSessionOptions options = ClientSessionOptions.builder()
                .causallyConsistent(true)
                .defaultTransactionOptions(TransactionOptions.builder().readConcern(ReadConcern.LINEARIZABLE).build())
                .build();
        expected = mongoCluster.getClientSessionHelper().createClientSessionMono(options, OPERATION_EXECUTOR);
        assertPublisherIsTheSameAs(expected, mongoCluster.startSession(options), "with options");

    }

    private MongoClusterImpl createMongoCluster() {
        return new MongoClusterImpl(mock(Cluster.class), null, OPERATION_EXECUTOR, mock(ServerSessionPool.class),
                mock(ClientSessionHelper.class), OPERATION_PUBLISHER);
    }
}
