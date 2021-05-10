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
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.TransactionOptions;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class MongoClientImplTest extends TestHelper {

    @Mock
    private ClientSession clientSession;

    private final MongoClientImpl mongoClient = createMongoClient();
    private final MongoOperationPublisher<Document> mongoOperationPublisher = mongoClient.getMongoOperationPublisher();

    @Test
    void testListDatabases() {
        assertAll("listDatabases",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.listDatabases((Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.listDatabases((ClientSession) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> mongoClient.listDatabases(clientSession, null))),
                  () -> {
                      ListDatabasesPublisher<Document> expected =
                              new ListDatabasesPublisherImpl<>(null, mongoOperationPublisher);
                      assertPublisherIsTheSameAs(expected, mongoClient.listDatabases(), "Default");
                  },
                  () -> {
                      ListDatabasesPublisher<Document> expected =
                              new ListDatabasesPublisherImpl<>(clientSession, mongoOperationPublisher);
                      assertPublisherIsTheSameAs(expected, mongoClient.listDatabases(clientSession), "With session");
                  },
                  () -> {
                      ListDatabasesPublisher<BsonDocument> expected =
                              new ListDatabasesPublisherImpl<>(null, mongoOperationPublisher
                                      .withDocumentClass(BsonDocument.class));
                      assertPublisherIsTheSameAs(expected, mongoClient.listDatabases(BsonDocument.class), "Alternative class");
                  },
                  () -> {
                      ListDatabasesPublisher<BsonDocument> expected =
                              new ListDatabasesPublisherImpl<>(clientSession, mongoOperationPublisher
                                      .withDocumentClass(BsonDocument.class));
                      assertPublisherIsTheSameAs(expected, mongoClient.listDatabases(clientSession, BsonDocument.class),
                                                 "Alternative class with session");
                  }
        );
    }

    @Test
    void testListDatabaseNames() {
        assertAll("listDatabaseNames",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.listDatabaseNames(null))),
                  () -> {
                      ListDatabasesPublisher<Document> expected =
                              new ListDatabasesPublisherImpl<>(null, mongoOperationPublisher).nameOnly(true);

                      assertPublisherIsTheSameAs(expected, mongoClient.listDatabaseNames(), "Default");
                  },
                  () -> {
                      ListDatabasesPublisher<Document> expected =
                              new ListDatabasesPublisherImpl<>(clientSession, mongoOperationPublisher).nameOnly(true);

                      assertPublisherIsTheSameAs(expected, mongoClient.listDatabaseNames(clientSession), "With session");
                  }
        );
    }

    @Test
    void testWatch() {
        List<Bson> pipeline = singletonList(BsonDocument.parse("{$match: {open: true}}"));
        assertAll("watch",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.watch((Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.watch((List<Bson>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.watch(pipeline, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.watch((ClientSession) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.watch(null, pipeline)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> mongoClient.watch(null, pipeline, Document.class))
                  ),
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher.withDatabase("admin"),
                                                              Document.class, emptyList(), ChangeStreamLevel.CLIENT);
                      assertPublisherIsTheSameAs(expected, mongoClient.watch(), "Default");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher.withDatabase("admin"),
                                                              Document.class, pipeline, ChangeStreamLevel.CLIENT);
                      assertPublisherIsTheSameAs(expected, mongoClient.watch(pipeline), "With pipeline");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher.withDatabase("admin"),
                                                              BsonDocument.class, emptyList(), ChangeStreamLevel.CLIENT);
                      assertPublisherIsTheSameAs(expected, mongoClient.watch(BsonDocument.class),
                                                 "With result class");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher.withDatabase("admin"),
                                                              BsonDocument.class, pipeline, ChangeStreamLevel.CLIENT);
                      assertPublisherIsTheSameAs(expected, mongoClient.watch(pipeline, BsonDocument.class),
                                                 "With pipeline & result class");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              new ChangeStreamPublisherImpl<>(clientSession, mongoOperationPublisher.withDatabase("admin"),
                                                              Document.class, emptyList(), ChangeStreamLevel.CLIENT);
                      assertPublisherIsTheSameAs(expected, mongoClient.watch(clientSession), "with session");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              new ChangeStreamPublisherImpl<>(clientSession, mongoOperationPublisher.withDatabase("admin"),
                                                              Document.class, pipeline, ChangeStreamLevel.CLIENT);
                      assertPublisherIsTheSameAs(expected, mongoClient.watch(clientSession, pipeline), "With session & pipeline");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              new ChangeStreamPublisherImpl<>(clientSession, mongoOperationPublisher.withDatabase("admin"),
                                                              BsonDocument.class, emptyList(), ChangeStreamLevel.CLIENT);
                      assertPublisherIsTheSameAs(expected, mongoClient.watch(clientSession, BsonDocument.class),
                                                 "With session & resultClass");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              new ChangeStreamPublisherImpl<>(clientSession, mongoOperationPublisher.withDatabase("admin"),
                                                              BsonDocument.class, pipeline, ChangeStreamLevel.CLIENT);
                      assertPublisherIsTheSameAs(expected, mongoClient.watch(clientSession, pipeline, BsonDocument.class),
                                                 "With clientSession, pipeline & result class");
                  }
        );
    }

    @Test
    void testStartSession() {
        ServerDescription serverDescription = ServerDescription.builder()
                .address(new ServerAddress())
                .state(ServerConnectionState.CONNECTED)
                .maxWireVersion(8)
                .build();

        MongoClientImpl mongoClient = createMongoClient();
        Cluster cluster = mongoClient.getCluster();
        when(cluster.getCurrentDescription())
                .thenReturn(new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.STANDALONE, singletonList(serverDescription)));

        ServerSessionPool serverSessionPool = mock(ServerSessionPool.class);
        ClientSessionHelper clientSessionHelper = new ClientSessionHelper(mongoClient, serverSessionPool);

        assertAll("Start Session Tests",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.startSession(null))
                  ),
                  () -> {
                      Mono<ClientSession> expected = clientSessionHelper
                              .createClientSessionMono(ClientSessionOptions.builder().build(), OPERATION_EXECUTOR);
                      assertPublisherIsTheSameAs(expected, mongoClient.startSession(), "Default");
                  },
                  () -> {
                      ClientSessionOptions options = ClientSessionOptions.builder()
                              .causallyConsistent(true)
                              .defaultTransactionOptions(TransactionOptions.builder().readConcern(ReadConcern.LINEARIZABLE).build())
                              .build();
                      Mono<ClientSession> expected =
                              clientSessionHelper.createClientSessionMono(options, OPERATION_EXECUTOR);
                      assertPublisherIsTheSameAs(expected, mongoClient.startSession(options), "with options");
                  });
    }

    @Test
    void testTimeoutMSNotSupported() {
        assertThrows(UnsupportedOperationException.class, () ->
            new MongoClientImpl(MongoClientSettings.builder().timeout(0, TimeUnit.MILLISECONDS).build(),
                    mock(Cluster.class),
                    OPERATION_EXECUTOR)
        );
    }

    private MongoClientImpl createMongoClient() {
        return new MongoClientImpl(MongoClientSettings.builder().build(), mock(Cluster.class), OPERATION_EXECUTOR);
    }
}
