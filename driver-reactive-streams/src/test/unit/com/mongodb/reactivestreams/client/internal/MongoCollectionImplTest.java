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

import com.mongodb.CreateIndexCommitQuorum;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropCollectionOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.ListIndexesPublisher;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class MongoCollectionImplTest extends TestHelper {
    @Mock
    private ClientSession clientSession;

    private final MongoCollectionImpl<Document> collection =
            new MongoCollectionImpl<>(OPERATION_PUBLISHER.withNamespace(new MongoNamespace("db.coll")));
    private final MongoOperationPublisher<Document> mongoOperationPublisher = collection.getPublisherHelper();

    private final Bson filter = BsonDocument.parse("{$match: {open: true}}");
    private final List<Bson> pipeline = singletonList(filter);
    private final Collation collation = Collation.builder().locale("de").build();

    @Test
    void testAggregate() {
        assertAll("Aggregate tests",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.aggregate(null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.aggregate(clientSession, null))
                  ),
                  () -> {
                      AggregatePublisher<Document> expected =
                              new AggregatePublisherImpl<>(null, mongoOperationPublisher, pipeline, AggregationLevel.COLLECTION);
                      assertPublisherIsTheSameAs(expected, collection.aggregate(pipeline), "Default");
                  },
                  () -> {
                      AggregatePublisher<BsonDocument> expected =
                              new AggregatePublisherImpl<>(null, mongoOperationPublisher.withDocumentClass(BsonDocument.class),
                                                           pipeline, AggregationLevel.COLLECTION);
                      assertPublisherIsTheSameAs(expected, collection.aggregate(pipeline, BsonDocument.class),
                                                 "With result class");
                  },
                  () -> {
                      AggregatePublisher<Document> expected =
                              new AggregatePublisherImpl<>(clientSession, mongoOperationPublisher, pipeline, AggregationLevel.COLLECTION);
                      assertPublisherIsTheSameAs(expected, collection.aggregate(clientSession, pipeline), "With session");
                  },
                  () -> {
                      AggregatePublisher<BsonDocument> expected =
                              new AggregatePublisherImpl<>(clientSession, mongoOperationPublisher.withDocumentClass(BsonDocument.class),
                                                           pipeline, AggregationLevel.COLLECTION);
                      assertPublisherIsTheSameAs(expected, collection.aggregate(clientSession, pipeline, BsonDocument.class),
                                                 "With session & result class");
                  }
        );
    }

    @Test
    public void testBulkWrite() {
        List<WriteModel<Document>> requests = singletonList(new InsertOneModel<>(new Document()));
        BulkWriteOptions options = new BulkWriteOptions().ordered(false);

        assertAll("bulkWrite",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.bulkWrite(null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.bulkWrite(requests, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.bulkWrite(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.bulkWrite(clientSession, requests, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.bulkWrite(null, requests)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.bulkWrite(null, requests, options))
                  ),
                  () -> {
                      Publisher<BulkWriteResult> expected = mongoOperationPublisher.bulkWrite(null, requests, new BulkWriteOptions());
                      assertPublisherIsTheSameAs(expected, collection.bulkWrite(requests), "Default");
                  },
                  () -> {
                      Publisher<BulkWriteResult> expected = mongoOperationPublisher.bulkWrite(null, requests, options);
                      assertPublisherIsTheSameAs(expected, collection.bulkWrite(requests, options), "With options");
                  },
                  () -> {
                      Publisher<BulkWriteResult> expected =
                              mongoOperationPublisher.bulkWrite(clientSession, requests, new BulkWriteOptions());
                      assertPublisherIsTheSameAs(expected, collection.bulkWrite(clientSession, requests), "With client session");
                  },
                  () -> {
                      Publisher<BulkWriteResult> expected = mongoOperationPublisher.bulkWrite(clientSession, requests, options);
                      assertPublisherIsTheSameAs(expected, collection.bulkWrite(clientSession, requests, options),
                                                 "With client session & options");
                  }
        );
    }

    @Test
    public void testCountDocuments() {
        CountOptions options = new CountOptions().collation(Collation.builder().locale("de").build());

        assertAll("countDocuments",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.countDocuments((Bson) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.countDocuments(filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.countDocuments((ClientSession) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.countDocuments(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.countDocuments(clientSession, filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.countDocuments(null, filter)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.countDocuments(null, filter, options))
                  ),
                  () -> {
                      Publisher<Long> expected = mongoOperationPublisher.countDocuments(null, new BsonDocument(), new CountOptions());
                      assertPublisherIsTheSameAs(expected, collection.countDocuments(), "Default");
                  },
                  () -> {
                      Publisher<Long> expected = mongoOperationPublisher.countDocuments(null, filter, new CountOptions());
                      assertPublisherIsTheSameAs(expected, collection.countDocuments(filter), "With filter");
                  },
                  () -> {
                      Publisher<Long> expected = mongoOperationPublisher.countDocuments(null, filter, options);
                      assertPublisherIsTheSameAs(expected, collection.countDocuments(filter, options), "With filter & options");
                  },
                  () -> {
                      Publisher<Long> expected =
                              mongoOperationPublisher.countDocuments(clientSession, new BsonDocument(), new CountOptions());
                      assertPublisherIsTheSameAs(expected, collection.countDocuments(clientSession), "With client session");
                  },
                  () -> {
                      Publisher<Long> expected = mongoOperationPublisher.countDocuments(clientSession, filter, new CountOptions());
                      assertPublisherIsTheSameAs(expected, collection.countDocuments(clientSession, filter),
                                                 "With client session & filter");
                  },
                  () -> {
                      Publisher<Long> expected = mongoOperationPublisher.countDocuments(clientSession, filter, options);
                      assertPublisherIsTheSameAs(expected, collection.countDocuments(clientSession, filter, options),
                                                 "With client session, filter & options");
                  }
        );
    }

    @Test
    public void testCreateIndex() {
        Bson key = BsonDocument.parse("{key: 1}");
        IndexOptions indexOptions = new IndexOptions();
        IndexOptions customOptions = new IndexOptions().background(true).bits(9);


        assertAll("createIndex",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndex(null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndex(key, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndex(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndex(clientSession, key, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndex(null, key)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndex(null, key, indexOptions))
                  ),
                  () -> {
                      Publisher<String> expected = mongoOperationPublisher.createIndex(null, key, new IndexOptions());
                      assertPublisherIsTheSameAs(expected, collection.createIndex(key), "Default");
                  },
                  () -> {
                      Publisher<String> expected = mongoOperationPublisher.createIndex(null, key, customOptions);
                      assertPublisherIsTheSameAs(expected, collection.createIndex(key, customOptions), "With custom options");
                  },
                  () -> {
                      Publisher<String> expected = mongoOperationPublisher.createIndex(clientSession, key, new IndexOptions());
                      assertPublisherIsTheSameAs(expected, collection.createIndex(clientSession, key), "With client session");
                  },
                  () -> {
                      Publisher<String> expected = mongoOperationPublisher.createIndex(clientSession, key, customOptions);
                      assertPublisherIsTheSameAs(expected, collection.createIndex(clientSession, key, customOptions),
                                                 "With client session, & custom options");
                  }
        );
    }

    @Test
    public void testCreateIndexes() {
        Bson key = BsonDocument.parse("{key: 1}");
        CreateIndexOptions createIndexOptions = new CreateIndexOptions();
        CreateIndexOptions customCreateIndexOptions = new CreateIndexOptions().commitQuorum(CreateIndexCommitQuorum.VOTING_MEMBERS);
        List<IndexModel> indexes = singletonList(new IndexModel(key, new IndexOptions().background(true).bits(9)));
        assertAll("createIndexes",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndexes(null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndexes(indexes, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndexes(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndexes(clientSession, indexes, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndexes(null, indexes)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndexes(null, indexes, createIndexOptions))
                  ),
                  () -> {
                      Publisher<String> expected = mongoOperationPublisher.createIndexes(null, indexes, createIndexOptions);
                      assertPublisherIsTheSameAs(expected, collection.createIndexes(indexes), "Default");
                  },
                  () -> {
                      Publisher<String> expected = mongoOperationPublisher.createIndexes(null, indexes, customCreateIndexOptions);
                      assertPublisherIsTheSameAs(expected, collection.createIndexes(indexes, customCreateIndexOptions),
                                                 "With custom options");
                  },
                  () -> {
                      Publisher<String> expected =
                              mongoOperationPublisher.createIndexes(clientSession, indexes, createIndexOptions);
                      assertPublisherIsTheSameAs(expected, collection.createIndexes(clientSession, indexes), "With client session");
                  },
                  () -> {
                      Publisher<String> expected =
                              mongoOperationPublisher.createIndexes(clientSession, indexes, customCreateIndexOptions);
                      assertPublisherIsTheSameAs(expected, collection.createIndexes(clientSession, indexes, customCreateIndexOptions),
                                                 "With client session, & custom options");
                  }
        );
    }

    @Test
    public void testDeleteOne() {
        DeleteOptions customOptions = new DeleteOptions().collation(collation);
        assertAll("deleteOne",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.deleteOne(null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.deleteOne(filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.deleteOne(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.deleteOne(clientSession, filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.deleteOne(null, filter)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.deleteOne(clientSession, filter, null))
                  ),
                  () -> {
                      Publisher<DeleteResult> expected = mongoOperationPublisher.deleteOne(null, filter, new DeleteOptions());
                      assertPublisherIsTheSameAs(expected, collection.deleteOne(filter), "Default");
                  },
                  () -> {
                      Publisher<DeleteResult> expected = mongoOperationPublisher.deleteOne(null, filter, customOptions);
                      assertPublisherIsTheSameAs(expected, collection.deleteOne(filter, customOptions), "With options");
                  },
                  () -> {
                      Publisher<DeleteResult> expected = mongoOperationPublisher.deleteOne(clientSession, filter, new DeleteOptions());
                      assertPublisherIsTheSameAs(expected, collection.deleteOne(clientSession, filter), "With client session");
                  },
                  () -> {
                      Publisher<DeleteResult> expected = mongoOperationPublisher.deleteOne(clientSession, filter, customOptions);
                      assertPublisherIsTheSameAs(expected, collection.deleteOne(clientSession, filter, customOptions),
                                                 "With client session & options");
                  }
        );
    }

    @Test
    public void testDeleteMany() {
        DeleteOptions customOptions = new DeleteOptions().collation(collation);
        assertAll("deleteMany",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.deleteMany(null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.deleteMany(filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.deleteMany(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.deleteMany(clientSession, filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.deleteMany(null, filter)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.deleteMany(clientSession, filter, null))
                  ),

                  () -> {
                      Publisher<DeleteResult> expected = mongoOperationPublisher.deleteMany(null, filter, new DeleteOptions());
                      assertPublisherIsTheSameAs(expected, collection.deleteMany(filter), "Default");
                  },
                  () -> {
                      Publisher<DeleteResult> expected = mongoOperationPublisher.deleteMany(null, filter, customOptions);
                      assertPublisherIsTheSameAs(expected, collection.deleteMany(filter, customOptions), "With options");
                  },
                  () -> {
                      Publisher<DeleteResult> expected = mongoOperationPublisher.deleteMany(clientSession, filter, new DeleteOptions());
                      assertPublisherIsTheSameAs(expected, collection.deleteMany(clientSession, filter), "With client session");
                  },
                  () -> {
                      Publisher<DeleteResult> expected = mongoOperationPublisher.deleteMany(clientSession, filter, customOptions);
                      assertPublisherIsTheSameAs(expected, collection.deleteMany(clientSession, filter, customOptions),
                                                 "With client session & options");
                  }
        );
    }

    @Test
    public void testDistinct() {
        String fieldName = "fieldName";
        assertAll("distinct",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.distinct(null, Document.class)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.distinct(fieldName, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.distinct(fieldName, null, Document.class)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.distinct(clientSession, null, Document.class)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.distinct(clientSession, fieldName, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.distinct(clientSession, fieldName, null, Document.class)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.distinct(null, fieldName, Document.class)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.distinct(null, fieldName, filter, Document.class))
                  ),
                  () -> {
                      DistinctPublisher<Document> expected =
                              new DistinctPublisherImpl<>(null, mongoOperationPublisher, fieldName, new BsonDocument());
                      assertPublisherIsTheSameAs(expected, collection.distinct(fieldName, Document.class), "Default");
                  },
                  () -> {
                      DistinctPublisher<BsonDocument> expected =
                              new DistinctPublisherImpl<>(null, mongoOperationPublisher.withDocumentClass(BsonDocument.class),
                                                          fieldName, filter);
                      assertPublisherIsTheSameAs(expected, collection.distinct(fieldName, filter, BsonDocument.class),
                                                 "With filter & result class");
                  },
                  () -> {
                      DistinctPublisher<Document> expected =
                              new DistinctPublisherImpl<>(clientSession, mongoOperationPublisher, fieldName, new BsonDocument());
                      assertPublisherIsTheSameAs(expected, collection.distinct(fieldName, Document.class), "With client session");
                  },
                  () -> {
                      DistinctPublisher<BsonDocument> expected =
                              new DistinctPublisherImpl<>(clientSession, mongoOperationPublisher.withDocumentClass(BsonDocument.class),
                                                          fieldName, filter);
                      assertPublisherIsTheSameAs(expected, collection.distinct(fieldName, filter, BsonDocument.class),
                                                 "With client session, filter & result class");
                  }
        );
    }

    @Test
    public void testDrop() {
        DropCollectionOptions dropCollectionOptions = new DropCollectionOptions();
        assertAll("drop",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.drop(null, null))
                  ),
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.dropCollection(null, dropCollectionOptions);
                      assertPublisherIsTheSameAs(expected, collection.drop(), "Default");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.dropCollection(clientSession, dropCollectionOptions);
                      assertPublisherIsTheSameAs(expected, collection.drop(clientSession), "With client session");
                  },
                () -> {
                    Publisher<Void> expected = mongoOperationPublisher.dropCollection(null, dropCollectionOptions);
                    assertPublisherIsTheSameAs(expected, collection.drop(dropCollectionOptions), "Default");
                },
                () -> {
                    Publisher<Void> expected = mongoOperationPublisher.dropCollection(clientSession, dropCollectionOptions);
                    assertPublisherIsTheSameAs(expected, collection.drop(clientSession, dropCollectionOptions), "With client session");
                }
        );
    }

    @Test
    public void testDropIndex() {
        String indexName = "index_name";
        Bson index = Indexes.ascending("ascending_index");
        DropIndexOptions options = new DropIndexOptions().maxTime(1, TimeUnit.MILLISECONDS);
        assertAll("dropIndex",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.dropIndex((String) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.dropIndex((Bson) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.dropIndex(indexName, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.dropIndex(clientSession, (String) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.dropIndex(clientSession, (Bson) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.dropIndex(clientSession, (String) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.dropIndex(clientSession, indexName, null))

                  ),
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.dropIndex(null, indexName, new DropIndexOptions());
                      assertPublisherIsTheSameAs(expected, collection.dropIndex(indexName), "Default string");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.dropIndex(null, index, new DropIndexOptions());
                      assertPublisherIsTheSameAs(expected, collection.dropIndex(index), "Default bson");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.dropIndex(null, indexName, options);
                      assertPublisherIsTheSameAs(expected, collection.dropIndex(indexName, options), "With string & options");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.dropIndex(null, index, options);
                      assertPublisherIsTheSameAs(expected, collection.dropIndex(index, options), "With bson & options");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.dropIndex(clientSession, indexName, new DropIndexOptions());
                      assertPublisherIsTheSameAs(expected, collection.dropIndex(clientSession, indexName),
                                                 "With client session & string");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.dropIndex(clientSession, index, new DropIndexOptions());
                      assertPublisherIsTheSameAs(expected, collection.dropIndex(clientSession, index),
                                                 "With client session & bson");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.dropIndex(clientSession, indexName, options);
                      assertPublisherIsTheSameAs(expected, collection.dropIndex(clientSession, indexName, options),
                                                 "With client session, string & options");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.dropIndex(clientSession, index, options);
                      assertPublisherIsTheSameAs(expected, collection.dropIndex(clientSession, index, options),
                                                 "With client session, bson & options");
                  }
        );
    }

    @Test
    public void testDropIndexes() {
        DropIndexOptions options = new DropIndexOptions().maxTime(1, TimeUnit.MILLISECONDS);
        assertAll("dropIndexes",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.dropIndexes((DropIndexOptions) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.dropIndexes(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.dropIndexes(null, options))

                  ),
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.dropIndexes(null, new DropIndexOptions());
                      assertPublisherIsTheSameAs(expected, collection.dropIndexes(), "Default");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.dropIndexes(null, options);
                      assertPublisherIsTheSameAs(expected, collection.dropIndexes(options), "With options");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.dropIndexes(clientSession, new DropIndexOptions());
                      assertPublisherIsTheSameAs(expected, collection.dropIndexes(clientSession), "With client session");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.dropIndexes(clientSession, options);
                      assertPublisherIsTheSameAs(expected, collection.dropIndexes(clientSession, options),
                                                 "With client session & options");
                  }
        );
    }


    @Test
    public void testEstimatedDocumentCount() {
        EstimatedDocumentCountOptions options = new EstimatedDocumentCountOptions().maxTime(1, TimeUnit.MILLISECONDS);
        assertAll("estimatedDocumentCount",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.estimatedDocumentCount(null))
                  ),
                  () -> {
                      Publisher<Long> expected = mongoOperationPublisher.estimatedDocumentCount(new EstimatedDocumentCountOptions());
                      assertPublisherIsTheSameAs(expected, collection.estimatedDocumentCount(), "Default");
                  },
                  () -> {
                      Publisher<Long> expected = mongoOperationPublisher.estimatedDocumentCount(options);
                      assertPublisherIsTheSameAs(expected, collection.estimatedDocumentCount(options), "With options");
                  }
        );
    }

    @Test
    public void testFind() {
        assertAll("find",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.find((Bson) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.find((Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.find(filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.find(clientSession, (Bson) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.find(clientSession, (Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.find(clientSession, filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.find(null, filter)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.find((ClientSession) null, Document.class)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.find(null, filter, Document.class))
                  ),
                  () -> {
                      FindPublisher<Document> expected =
                              new FindPublisherImpl<>(null, mongoOperationPublisher, new BsonDocument());
                      assertPublisherIsTheSameAs(expected, collection.find(), "Default");
                  },
                  () -> {
                      FindPublisher<Document> expected =
                              new FindPublisherImpl<>(null, mongoOperationPublisher, filter);
                      assertPublisherIsTheSameAs(expected, collection.find(filter), "With filter");
                  },
                  () -> {
                      FindPublisher<BsonDocument> expected =
                              new FindPublisherImpl<>(null, mongoOperationPublisher.withDocumentClass(BsonDocument.class), filter);
                      assertPublisherIsTheSameAs(expected, collection.find(filter, BsonDocument.class), "With filter & result class");
                  },
                  () -> {
                      FindPublisher<Document> expected =
                              new FindPublisherImpl<>(clientSession, mongoOperationPublisher, new BsonDocument());
                      assertPublisherIsTheSameAs(expected, collection.find(clientSession), "With client session");
                  },
                  () -> {
                      FindPublisher<Document> expected =
                              new FindPublisherImpl<>(clientSession, mongoOperationPublisher, filter);
                      assertPublisherIsTheSameAs(expected, collection.find(clientSession, filter), "With client session & filter");
                  },
                  () -> {
                      FindPublisher<BsonDocument> expected =
                              new FindPublisherImpl<>(clientSession, mongoOperationPublisher.withDocumentClass(BsonDocument.class), filter);
                      assertPublisherIsTheSameAs(expected, collection.find(clientSession, filter, BsonDocument.class),
                                                 "With client session, filter & result class");
                  }
        );
    }

    @Test
    public void testFindOneAndDelete() {
        FindOneAndDeleteOptions options = new FindOneAndDeleteOptions().collation(collation);
        assertAll("findOneAndDelete",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.findOneAndDelete(null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.findOneAndDelete(filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndDelete(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndDelete(clientSession, filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndDelete(null, filter)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndDelete(null, filter, options))
                  ),
                  () -> {
                      Publisher<Document> expected = mongoOperationPublisher.findOneAndDelete(null, filter, new FindOneAndDeleteOptions());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndDelete(filter), "Default");
                  },
                  () -> {
                      Publisher<Document> expected = mongoOperationPublisher.findOneAndDelete(null, filter, options);
                      assertPublisherIsTheSameAs(expected, collection.findOneAndDelete(filter, options), "With filter & options");
                  },
                  () -> {
                      Publisher<Document> expected =
                              mongoOperationPublisher.findOneAndDelete(clientSession, filter, new FindOneAndDeleteOptions());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndDelete(clientSession, filter), "With client session");
                  },
                  () -> {
                      Publisher<Document> expected = mongoOperationPublisher.findOneAndDelete(clientSession, filter, options);
                      assertPublisherIsTheSameAs(expected, collection.findOneAndDelete(clientSession, filter, options),
                                                 "With client session, filter & options");
                  }
        );
    }

    @Test
    public void testFindOneAndReplace() {
        FindOneAndReplaceOptions options = new FindOneAndReplaceOptions().collation(collation);
        Document replacement = new Document();
        assertAll("findOneAndReplace",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.findOneAndReplace(null, replacement)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndReplace(filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndReplace(clientSession, null, replacement)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndReplace(clientSession, filter, replacement, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndReplace(null, filter, replacement)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndReplace(null, filter, replacement, options))
                  ),
                  () -> {
                      Publisher<Document> expected =
                              mongoOperationPublisher.findOneAndReplace(null, filter, replacement, new FindOneAndReplaceOptions());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndReplace(filter, replacement), "Default");
                  },
                  () -> {
                      Publisher<Document> expected = mongoOperationPublisher.findOneAndReplace(null, filter, replacement, options);
                      assertPublisherIsTheSameAs(expected, collection.findOneAndReplace(filter, replacement, options),
                                                 "With filter & options");
                  },
                  () -> {
                      Publisher<Document> expected =
                              mongoOperationPublisher.findOneAndReplace(clientSession, filter, replacement, new FindOneAndReplaceOptions());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndReplace(clientSession, filter, replacement),
                                                 "With client session");
                  },
                  () -> {
                      Publisher<Document> expected = mongoOperationPublisher.findOneAndReplace(clientSession, filter, replacement, options);
                      assertPublisherIsTheSameAs(expected, collection.findOneAndReplace(clientSession, filter, replacement, options),
                                                 "With client session, filter & options");
                  }
        );
    }

    @Test
    public void testFindOneAndUpdate() {
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().collation(collation);
        Document update = new Document();
        assertAll("findOneAndUpdate",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.findOneAndUpdate(null, update)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndUpdate(filter, (Bson) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndUpdate(clientSession, null, update)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndUpdate(clientSession, filter, update, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndUpdate(null, filter, update)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndUpdate(null, filter, update, options))
                  ),
                  () -> {
                      Publisher<Document> expected =
                              mongoOperationPublisher.findOneAndUpdate(null, filter, update, new FindOneAndUpdateOptions());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndUpdate(filter, update), "Default");
                  },
                  () -> {
                      Publisher<Document> expected = mongoOperationPublisher.findOneAndUpdate(null, filter, update, options);
                      assertPublisherIsTheSameAs(expected, collection.findOneAndUpdate(filter, update, options),
                                                 "With filter & options");
                  },
                  () -> {
                      Publisher<Document> expected =
                              mongoOperationPublisher.findOneAndUpdate(clientSession, filter, update, new FindOneAndUpdateOptions());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndUpdate(clientSession, filter, update),
                                                 "With client session");
                  },
                  () -> {
                      Publisher<Document> expected = mongoOperationPublisher.findOneAndUpdate(clientSession, filter, update, options);
                      assertPublisherIsTheSameAs(expected, collection.findOneAndUpdate(clientSession, filter, update, options),
                                                 "With client session, filter & options");
                  }
        );
    }

    @Test
    public void testInsertOne() {
        InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
        Document insert = new Document("_id", 1);
        assertAll("insertOne",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertOne(null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertOne(insert, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertOne(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.insertOne(clientSession, insert, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertOne(null, insert)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertOne(null, insert, options))
                  ),
                  () -> {
                      Publisher<InsertOneResult> expected = mongoOperationPublisher.insertOne(null, insert, new InsertOneOptions());
                      assertPublisherIsTheSameAs(expected, collection.insertOne(insert), "Default");
                  },
                  () -> {
                      Publisher<InsertOneResult> expected = mongoOperationPublisher.insertOne(null, insert, options);
                      assertPublisherIsTheSameAs(expected, collection.insertOne(insert, options), "With options");
                  },
                  () -> {
                      Publisher<InsertOneResult> expected =
                              mongoOperationPublisher.insertOne(clientSession, insert, new InsertOneOptions());
                      assertPublisherIsTheSameAs(expected, collection.insertOne(clientSession, insert), "With client session");
                  },
                  () -> {
                      Publisher<InsertOneResult> expected = mongoOperationPublisher.insertOne(clientSession, insert, options);
                      assertPublisherIsTheSameAs(expected, collection.insertOne(clientSession, insert, options),
                                                 "With client session & options");
                  }
        );
    }

    @Test
    public void testInsertMany() {
        InsertManyOptions options = new InsertManyOptions().bypassDocumentValidation(true);
        List<Document> inserts = singletonList(new Document("_id", 1));
        assertAll("insertMany",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertMany(null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertMany(inserts, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertMany(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.insertMany(clientSession, inserts, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertMany(null, inserts)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertMany(null, inserts, options))
                  ),
                  () -> {
                      Publisher<InsertManyResult> expected = mongoOperationPublisher.insertMany(null, inserts, new InsertManyOptions());
                      assertPublisherIsTheSameAs(expected, collection.insertMany(inserts), "Default");
                  },
                  () -> {
                      Publisher<InsertManyResult> expected = mongoOperationPublisher.insertMany(null, inserts, options);
                      assertPublisherIsTheSameAs(expected, collection.insertMany(inserts, options), "With options");
                  },
                  () -> {
                      Publisher<InsertManyResult> expected =
                              mongoOperationPublisher.insertMany(clientSession, inserts, new InsertManyOptions());
                      assertPublisherIsTheSameAs(expected, collection.insertMany(clientSession, inserts), "With client session");
                  },
                  () -> {
                      Publisher<InsertManyResult> expected = mongoOperationPublisher.insertMany(clientSession, inserts, options);
                      assertPublisherIsTheSameAs(expected, collection.insertMany(clientSession, inserts, options),
                                                 "With client session & options");
                  }
        );
    }

    @Test
    public void testListIndexes() {
        assertAll("listIndexes",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.listIndexes((Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.listIndexes(null, Document.class)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.listIndexes(clientSession, null))
                  ),
                  () -> {
                      ListIndexesPublisher<Document> expected =
                              new ListIndexesPublisherImpl<>(null, mongoOperationPublisher);
                      assertPublisherIsTheSameAs(expected, collection.listIndexes(), "Default");
                  },
                  () -> {
                      ListIndexesPublisher<BsonDocument> expected =
                              new ListIndexesPublisherImpl<>(null, mongoOperationPublisher.withDocumentClass(BsonDocument.class));
                      assertPublisherIsTheSameAs(expected, collection.listIndexes(BsonDocument.class), "With result class");
                  },
                  () -> {
                      ListIndexesPublisher<Document> expected =
                              new ListIndexesPublisherImpl<>(clientSession, mongoOperationPublisher);
                      assertPublisherIsTheSameAs(expected, collection.listIndexes(clientSession), "With client session");
                  },
                  () -> {
                      ListIndexesPublisher<BsonDocument> expected =
                              new ListIndexesPublisherImpl<>(clientSession, mongoOperationPublisher.withDocumentClass(BsonDocument.class));
                      assertPublisherIsTheSameAs(expected, collection.listIndexes(clientSession, BsonDocument.class),
                                                 "With client session & result class");
                  }
        );
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testMapReduce() {
        String map = "map";
        String reduce = "reduce";

        assertAll("mapReduce",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.mapReduce(null, reduce)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.mapReduce(map, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.mapReduce(map, reduce, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.mapReduce(clientSession, null, reduce)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.mapReduce(clientSession, map, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.mapReduce(clientSession, map, reduce, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.mapReduce(null, map, reduce)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.mapReduce(null, map, reduce, Document.class))
                  ),
                  () -> {
                      com.mongodb.reactivestreams.client.MapReducePublisher<Document> expected =
                              new MapReducePublisherImpl<>(null, mongoOperationPublisher, map, reduce);
                      assertPublisherIsTheSameAs(expected, collection.mapReduce(map, reduce), "Default");
                  },
                  () -> {
                      com.mongodb.reactivestreams.client.MapReducePublisher<BsonDocument> expected =
                              new MapReducePublisherImpl<>(null, mongoOperationPublisher.withDocumentClass(BsonDocument.class),
                                                           map, reduce);
                      assertPublisherIsTheSameAs(expected, collection.mapReduce(map, reduce, BsonDocument.class),
                                                 "With result class");
                  },
                  () -> {
                      com.mongodb.reactivestreams.client.MapReducePublisher<Document> expected =
                              new MapReducePublisherImpl<>(clientSession, mongoOperationPublisher, map, reduce);
                      assertPublisherIsTheSameAs(expected, collection.mapReduce(clientSession, map, reduce), "With client session");
                  },
                  () -> {
                      com.mongodb.reactivestreams.client.MapReducePublisher<BsonDocument> expected =
                              new MapReducePublisherImpl<>(clientSession, mongoOperationPublisher.withDocumentClass(BsonDocument.class),
                                                           map, reduce);
                      assertPublisherIsTheSameAs(expected, collection.mapReduce(clientSession, map, reduce, BsonDocument.class),
                                                 "With client session & result class");
                  }
        );
    }

    @Test
    public void testRenameCollection() {
        MongoNamespace mongoNamespace = new MongoNamespace("db2.coll2");
        RenameCollectionOptions options = new RenameCollectionOptions().dropTarget(true);
        assertAll("renameCollection",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.renameCollection(null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.renameCollection(mongoNamespace, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.renameCollection(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.renameCollection(clientSession, mongoNamespace, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.renameCollection(null, mongoNamespace)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.renameCollection(null, mongoNamespace, options))
                  ),
                  () -> {
                      Publisher<Void> expected =
                              mongoOperationPublisher.renameCollection(clientSession, mongoNamespace, new RenameCollectionOptions());
                      assertPublisherIsTheSameAs(expected, collection.renameCollection(mongoNamespace), "Default");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.renameCollection(null, mongoNamespace, options);
                      assertPublisherIsTheSameAs(expected, collection.renameCollection(mongoNamespace, options), "With options");
                  },
                  () -> {
                      Publisher<Void> expected =
                              mongoOperationPublisher.renameCollection(clientSession, mongoNamespace, new RenameCollectionOptions());
                      assertPublisherIsTheSameAs(expected, collection.renameCollection(clientSession, mongoNamespace),
                                                 "With client session");
                  },
                  () -> {
                      Publisher<Void> expected = mongoOperationPublisher.renameCollection(clientSession, mongoNamespace, options);
                      assertPublisherIsTheSameAs(expected, collection.renameCollection(clientSession, mongoNamespace, options),
                                                 "With client session & options");
                  }
        );
    }

    @Test
    public void testReplaceOne() {
        ReplaceOptions options = new ReplaceOptions().collation(collation);
        Document replacement = new Document();
        assertAll("replaceOne",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.replaceOne(null, replacement)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.replaceOne(filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.replaceOne(clientSession, null, replacement)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.replaceOne(clientSession, filter, replacement, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.replaceOne(null, filter, replacement)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.replaceOne(null, filter, replacement, options))
                  ),
                  () -> {
                      Publisher<UpdateResult> expected =
                              mongoOperationPublisher.replaceOne(null, filter, replacement, new ReplaceOptions());
                      assertPublisherIsTheSameAs(expected, collection.replaceOne(filter, replacement), "Default");
                  },
                  () -> {
                      Publisher<UpdateResult> expected = mongoOperationPublisher.replaceOne(null, filter, replacement, options);
                      assertPublisherIsTheSameAs(expected, collection.replaceOne(filter, replacement, options),
                                                 "With filter & options");
                  },
                  () -> {
                      Publisher<UpdateResult> expected =
                              mongoOperationPublisher.replaceOne(clientSession, filter, replacement, new ReplaceOptions());
                      assertPublisherIsTheSameAs(expected, collection.replaceOne(clientSession, filter, replacement),
                                                 "With client session");
                  },
                  () -> {
                      Publisher<UpdateResult> expected = mongoOperationPublisher.replaceOne(clientSession, filter, replacement, options);
                      assertPublisherIsTheSameAs(expected, collection.replaceOne(clientSession, filter, replacement, options),
                                                 "With client session, filter & options");
                  }
        );
    }

    @Test
    public void testUpdateOne() {
        UpdateOptions options = new UpdateOptions().collation(collation);
        Document update = new Document();
        assertAll("updateOne",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.updateOne(null, update)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateOne(filter, (Bson) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateOne(clientSession, null, update)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateOne(clientSession, filter, update, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateOne(null, filter, update)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateOne(null, filter, update, options))
                  ),
                  () -> {
                      Publisher<UpdateResult> expected = mongoOperationPublisher.updateOne(null, filter, update, new UpdateOptions());
                      assertPublisherIsTheSameAs(expected, collection.updateOne(filter, update), "Default");
                  },
                  () -> {
                      Publisher<UpdateResult> expected = mongoOperationPublisher.updateOne(null, filter, update, options);
                      assertPublisherIsTheSameAs(expected, collection.updateOne(filter, update, options),
                                                 "With filter & options");
                  },
                  () -> {
                      Publisher<UpdateResult> expected =
                              mongoOperationPublisher.updateOne(clientSession, filter, update, new UpdateOptions());
                      assertPublisherIsTheSameAs(expected, collection.updateOne(clientSession, filter, update),
                                                 "With client session");
                  },
                  () -> {
                      Publisher<UpdateResult> expected = mongoOperationPublisher.updateOne(clientSession, filter, update, options);
                      assertPublisherIsTheSameAs(expected, collection.updateOne(clientSession, filter, update, options),
                                                 "With client session, filter & options");
                  }
        );
    }

    @Test
    public void testUpdateMany() {
        UpdateOptions options = new UpdateOptions().collation(collation);
        List<Document> updates = singletonList(new Document());
        assertAll("updateMany",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.updateMany(null, updates)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateMany(filter, (Bson) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateMany(clientSession, null, updates)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateMany(clientSession, filter, updates, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateMany(null, filter, updates)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateMany(null, filter, updates, options))
                  ),
                  () -> {
                      Publisher<UpdateResult> expected = mongoOperationPublisher.updateMany(null, filter, updates, new UpdateOptions());
                      assertPublisherIsTheSameAs(expected, collection.updateMany(filter, updates), "Default");
                  },
                  () -> {
                      Publisher<UpdateResult> expected = mongoOperationPublisher.updateMany(null, filter, updates, options);
                      assertPublisherIsTheSameAs(expected, collection.updateMany(filter, updates, options),
                                                 "With filter & options");
                  },
                  () -> {
                      Publisher<UpdateResult> expected =
                              mongoOperationPublisher.updateMany(clientSession, filter, updates, new UpdateOptions());
                      assertPublisherIsTheSameAs(expected, collection.updateMany(clientSession, filter, updates),
                                                 "With client session");
                  },
                  () -> {
                      Publisher<UpdateResult> expected = mongoOperationPublisher.updateMany(clientSession, filter, updates, options);
                      assertPublisherIsTheSameAs(expected, collection.updateMany(clientSession, filter, updates, options),
                                                 "With client session, filter & options");
                  }
        );
    }

    @Test
    void testWatch() {
        List<Bson> pipeline = singletonList(BsonDocument.parse("{$match: {open: true}}"));
        assertAll("watch",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.watch((Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.watch((List<Bson>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.watch(pipeline, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.watch((ClientSession) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.watch(null, pipeline)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.watch(null, pipeline, Document.class))
                  ),
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher, Document.class, emptyList(),
                                                              ChangeStreamLevel.COLLECTION);
                      assertPublisherIsTheSameAs(expected, collection.watch(), "Default");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher, Document.class, pipeline,
                                                              ChangeStreamLevel.COLLECTION);
                      assertPublisherIsTheSameAs(expected, collection.watch(pipeline), "With pipeline");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher, BsonDocument.class, emptyList(),
                                                              ChangeStreamLevel.COLLECTION);
                      assertPublisherIsTheSameAs(expected, collection.watch(BsonDocument.class),
                                                 "With result class");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher, BsonDocument.class, pipeline,
                                                              ChangeStreamLevel.COLLECTION);
                      assertPublisherIsTheSameAs(expected, collection.watch(pipeline, BsonDocument.class),
                                                 "With pipeline & result class");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              new ChangeStreamPublisherImpl<>(clientSession, mongoOperationPublisher, Document.class, emptyList(),
                                                              ChangeStreamLevel.COLLECTION);
                      assertPublisherIsTheSameAs(expected, collection.watch(clientSession), "with session");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              new ChangeStreamPublisherImpl<>(clientSession, mongoOperationPublisher, Document.class, pipeline,
                                                              ChangeStreamLevel.COLLECTION);
                      assertPublisherIsTheSameAs(expected, collection.watch(clientSession, pipeline), "With session & pipeline");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              new ChangeStreamPublisherImpl<>(clientSession, mongoOperationPublisher, BsonDocument.class,
                                                              emptyList(), ChangeStreamLevel.COLLECTION);

                      assertPublisherIsTheSameAs(expected, collection.watch(clientSession, BsonDocument.class),
                                                 "With session & resultClass");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              new ChangeStreamPublisherImpl<>(clientSession, mongoOperationPublisher, BsonDocument.class, pipeline,
                                                              ChangeStreamLevel.COLLECTION);

                      assertPublisherIsTheSameAs(expected, collection.watch(clientSession, pipeline, BsonDocument.class),
                                                 "With clientSession, pipeline & result class");
                  }
        );
    }
}
