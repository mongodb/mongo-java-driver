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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListCollectionsPublisher;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.reactivestreams.Publisher;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class MongoDatabaseImplTest extends TestHelper {
    @Mock
    private ClientSession clientSession;

    private final MongoDatabaseImpl database = new MongoDatabaseImpl(OPERATION_PUBLISHER.withDatabase("db"));
    private final MongoOperationPublisher<Document> mongoOperationPublisher = database.getMongoOperationPublisher();

    @Test
    void testAggregate() {
        List<Bson> pipeline = singletonList(BsonDocument.parse("{$match: {open: true}}"));
        assertAll("Aggregate tests",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> database.aggregate(null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> database.aggregate(clientSession, null))
                  ),
                  () -> {
                      AggregatePublisher<Document> expected =
                              new AggregatePublisherImpl<>(null, mongoOperationPublisher, pipeline, AggregationLevel.DATABASE);
                      assertPublisherIsTheSameAs(expected, database.aggregate(pipeline), "Default");
                  },
                  () -> {
                      AggregatePublisher<BsonDocument> expected =
                              new AggregatePublisherImpl<>(null, mongoOperationPublisher.withDocumentClass(BsonDocument.class),
                                                           pipeline, AggregationLevel.DATABASE);
                      assertPublisherIsTheSameAs(expected, database.aggregate(pipeline, BsonDocument.class),
                                                 "With result class");
                  },
                  () -> {
                      AggregatePublisher<Document> expected =
                              new AggregatePublisherImpl<>(clientSession, mongoOperationPublisher,
                                                           pipeline, AggregationLevel.DATABASE);
                      assertPublisherIsTheSameAs(expected, database.aggregate(clientSession, pipeline), "With session");
                  },
                  () -> {
                      AggregatePublisher<BsonDocument> expected =
                              new AggregatePublisherImpl<>(clientSession, mongoOperationPublisher.withDocumentClass(BsonDocument.class),
                                                           pipeline, AggregationLevel.DATABASE);
                      assertPublisherIsTheSameAs(expected, database.aggregate(clientSession, pipeline, BsonDocument.class),
                                                 "With session & result class");
                  }
        );
    }

    @Test
    void shouldListCollections() {
        database.listCollections();

        assertAll("listCollections tests",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> database.listCollections((Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.listCollections((ClientSession) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.listCollections(clientSession, null))
                  ),
                  () -> {
                      ListCollectionsPublisher<Document> expected =
                              new ListCollectionsPublisherImpl<>(null, mongoOperationPublisher, false);
                      assertPublisherIsTheSameAs(expected, database.listCollections(), "Default");
                  },
                  () -> {
                      ListCollectionsPublisher<BsonDocument> expected =
                              new ListCollectionsPublisherImpl<>(null, mongoOperationPublisher.withDocumentClass(BsonDocument.class),
                                                                 false);
                      assertPublisherIsTheSameAs(expected, database.listCollections(BsonDocument.class), "With result class");
                  },
                  () -> {
                      ListCollectionsPublisher<Document> expected =
                              new ListCollectionsPublisherImpl<>(clientSession, mongoOperationPublisher, false);
                      assertPublisherIsTheSameAs(expected, database.listCollections(clientSession), "With client session");
                  },
                  () -> {
                      ListCollectionsPublisher<BsonDocument> expected =
                              new ListCollectionsPublisherImpl<>(clientSession,
                                                                 mongoOperationPublisher.withDocumentClass(BsonDocument.class), false);
                      assertPublisherIsTheSameAs(expected, database.listCollections(clientSession, BsonDocument.class),
                                                 "With client session & result class");
                  }
        );
    }

    @Test
    void testListCollectionNames() {
        assertAll("listCollectionNames",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> database.listCollectionNames(null))
                  ),
                  () -> {
                      ListCollectionsPublisher<Document> expected =
                              new ListCollectionsPublisherImpl<>(null, mongoOperationPublisher, true);
                      assertPublisherIsTheSameAs(expected, database.listCollectionNames(), "Default");
                  },
                  () -> {
                      ListCollectionsPublisher<Document> expected =
                              new ListCollectionsPublisherImpl<>(clientSession, mongoOperationPublisher, true);
                      assertPublisherIsTheSameAs(expected, database.listCollectionNames(clientSession), "With client session");
                  }
        );
    }

    @Test
    void testCreateCollection() {
        String collectionName = "coll";
        assertAll("createCollection",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> database.createCollection(null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.createCollection(collectionName, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.createCollection(null, collectionName)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.createCollection(clientSession, collectionName, null))
                  ),
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher
                              .createCollection(null, new MongoNamespace(database.getName(), collectionName),
                                                new CreateCollectionOptions());
                      assertPublisherIsTheSameAs(expected, database.createCollection(collectionName), "Default");
                  },
                  () -> {
                      CreateCollectionOptions options = new CreateCollectionOptions().sizeInBytes(500).capped(true);
                      Publisher<Void> expected = mongoOperationPublisher
                              .createCollection(null, new MongoNamespace(database.getName(), collectionName), options);
                      assertPublisherIsTheSameAs(expected, database.createCollection(collectionName, options), "With options");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher
                              .createCollection(clientSession, new MongoNamespace(database.getName(), collectionName),
                                                new CreateCollectionOptions());
                      assertPublisherIsTheSameAs(expected, database.createCollection(clientSession, collectionName),
                                                 "With client session");
                  },
                  () -> {
                      CreateCollectionOptions options = new CreateCollectionOptions().sizeInBytes(500).capped(true);
                      Publisher<Void> expected = mongoOperationPublisher
                              .createCollection(clientSession, new MongoNamespace(database.getName(), collectionName), options);
                      assertPublisherIsTheSameAs(expected, database.createCollection(clientSession, collectionName, options),
                                                 "With client session & options");
                  }
        );
    }

    @Test
    void testCreateView() {
        String viewName = "viewName";
        String viewOn = "viewOn";
        List<Bson> pipeline = singletonList(BsonDocument.parse("{$match: {open: true}}"));
        CreateViewOptions options = new CreateViewOptions().collation(Collation.builder().locale("de").build());

        assertAll("createView",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.createView(null, viewOn, pipeline)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.createView(viewName, null, pipeline)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.createView(viewName, viewOn, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.createView(viewName, viewOn, pipeline, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.createView(null, viewName, viewOn, pipeline)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.createView(null, viewName, viewOn, pipeline, options))

                  ),
                  () -> {
                      Publisher<Void> expected =
                              mongoOperationPublisher.createView(null, viewName, viewOn, pipeline, new CreateViewOptions());
                      assertPublisherIsTheSameAs(expected, database.createView(viewName, viewOn, pipeline), "Default");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.createView(null, viewName, viewOn, pipeline, options);
                      assertPublisherIsTheSameAs(expected, database.createView(viewName, viewOn, pipeline, options),
                                                 "With options");
                  },
                  () -> {
                      Publisher<Void> expected =
                              mongoOperationPublisher.createView(clientSession, viewName, viewOn, pipeline, new CreateViewOptions());
                      assertPublisherIsTheSameAs(expected, database.createView(clientSession, viewName, viewOn, pipeline),
                                                 "With client session");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.createView(clientSession, viewName, viewOn, pipeline, options);
                      assertPublisherIsTheSameAs(expected, database.createView(clientSession, viewName, viewOn, pipeline, options),
                                                 "With client session & options");
                  }
        );
    }

    @Test
    void testDrop() {
        assertAll("drop",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> database.drop(null))
                  ),
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.dropDatabase(null);
                      assertPublisherIsTheSameAs(expected, database.drop(), "Default");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.dropDatabase(clientSession);
                      assertPublisherIsTheSameAs(expected, database.drop(clientSession), "With client session");
                  }
        );
    }

    @Test
    void testRunCommand() {
        Bson command = BsonDocument.parse("{ping : 1}");

        assertAll("runCommand",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.runCommand(null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.runCommand(command, (ReadPreference) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.runCommand(command, (Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.runCommand(command, ReadPreference.nearest(), null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.runCommand(null, command)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.runCommand(null, command, ReadPreference.nearest())),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.runCommand(null, command, Document.class)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.runCommand(null, command, ReadPreference.nearest(), Document.class))
                  ),
                  () -> {
                      Publisher<Document> expected =
                              mongoOperationPublisher.runCommand(null, command, ReadPreference.primary(), Document.class);
                      assertPublisherIsTheSameAs(expected, database.runCommand(command), "Default");
                  },
                  () -> {
                      Publisher<BsonDocument> expected =
                              mongoOperationPublisher.runCommand(null, command, ReadPreference.primary(), BsonDocument.class);
                      assertPublisherIsTheSameAs(expected, database.runCommand(command, BsonDocument.class),
                                                 "With result class");
                  },
                  () -> {
                      Publisher<Document> expected =
                              mongoOperationPublisher.runCommand(null, command, ReadPreference.nearest(), Document.class);
                      assertPublisherIsTheSameAs(expected, database.runCommand(command, ReadPreference.nearest()),
                                                 "With read preference");
                  },
                  () -> {
                      Publisher<BsonDocument> expected =
                              mongoOperationPublisher.runCommand(null, command, ReadPreference.nearest(), BsonDocument.class);
                      assertPublisherIsTheSameAs(expected, database.runCommand(command, ReadPreference.nearest(), BsonDocument.class),
                                                 "With read preference & result class");
                  },
                  () -> {
                      Publisher<Document> expected =
                              mongoOperationPublisher.runCommand(clientSession, command, ReadPreference.primary(), Document.class);
                      assertPublisherIsTheSameAs(expected, database.runCommand(clientSession, command),
                                                 "With client session");
                  },
                  () -> {
                      Publisher<BsonDocument> expected = mongoOperationPublisher
                              .runCommand(clientSession, command, ReadPreference.primary(), BsonDocument.class);
                      assertPublisherIsTheSameAs(expected, database.runCommand(clientSession, command, BsonDocument.class),
                                                 "With client session & result class");
                  },
                  () -> {
                      Publisher<Document> expected =
                              mongoOperationPublisher.runCommand(clientSession, command, ReadPreference.nearest(), Document.class);
                      assertPublisherIsTheSameAs(expected, database.runCommand(clientSession, command, ReadPreference.nearest()),
                                                 "With client session & read preference");
                  },
                  () -> {
                      Publisher<BsonDocument> expected = mongoOperationPublisher
                              .runCommand(clientSession, command, ReadPreference.nearest(), BsonDocument.class);
                      assertPublisherIsTheSameAs(expected, database.runCommand(clientSession, command, ReadPreference.nearest(),
                                                                               BsonDocument.class),
                                                 "With client session, read preference & result class");
                  }
        );
    }

    @Test
    void testWatch() {
        List<Bson> pipeline = singletonList(BsonDocument.parse("{$match: {open: true}}"));
        assertAll("watch",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> database.watch((Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> database.watch((List<Bson>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> database.watch(pipeline, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> database.watch((ClientSession) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> database.watch(null, pipeline)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> database.watch(null, pipeline, Document.class))
                  ),
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher, Document.class, emptyList(),
                                                              ChangeStreamLevel.DATABASE);
                      assertPublisherIsTheSameAs(expected, database.watch(), "Default");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher, Document.class, pipeline,
                                                              ChangeStreamLevel.DATABASE);
                      assertPublisherIsTheSameAs(expected, database.watch(pipeline), "With pipeline");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher, BsonDocument.class, emptyList(),
                                                              ChangeStreamLevel.DATABASE);
                      assertPublisherIsTheSameAs(expected, database.watch(BsonDocument.class),
                                                 "With result class");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher, BsonDocument.class, pipeline,
                                                              ChangeStreamLevel.DATABASE);
                      assertPublisherIsTheSameAs(expected, database.watch(pipeline, BsonDocument.class),
                                                 "With pipeline & result class");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              new ChangeStreamPublisherImpl<>(clientSession, mongoOperationPublisher, Document.class, emptyList(),
                                                              ChangeStreamLevel.DATABASE);
                      assertPublisherIsTheSameAs(expected, database.watch(clientSession), "with session");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              new ChangeStreamPublisherImpl<>(clientSession, mongoOperationPublisher, Document.class, pipeline,
                                                              ChangeStreamLevel.DATABASE);
                      assertPublisherIsTheSameAs(expected, database.watch(clientSession, pipeline), "With session & pipeline");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              new ChangeStreamPublisherImpl<>(clientSession, mongoOperationPublisher, BsonDocument.class, emptyList(),
                                                              ChangeStreamLevel.DATABASE);
                      assertPublisherIsTheSameAs(expected, database.watch(clientSession, BsonDocument.class),
                                                 "With session & resultClass");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              new ChangeStreamPublisherImpl<>(clientSession, mongoOperationPublisher, BsonDocument.class, pipeline,
                                                              ChangeStreamLevel.DATABASE);
                      assertPublisherIsTheSameAs(expected, database.watch(clientSession, pipeline, BsonDocument.class),
                                                 "With clientSession, pipeline & result class");
                  }
        );
    }

}
