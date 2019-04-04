/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.internal


import com.mongodb.MongoClientSettings
import com.mongodb.MongoInternalException
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.client.model.AggregationLevel
import com.mongodb.client.model.changestream.ChangeStreamLevel
import com.mongodb.operation.BatchCursor
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.Document
import spock.lang.IgnoreIf
import spock.lang.Specification

import java.util.function.Consumer

import static com.mongodb.ReadPreference.secondary

class Java8MongoIterablesSpecification extends Specification {

    static final boolean IS_CONSUMER_CLASS_AVAILABLE

    static {
        try {
            Java8MongoIterablesSpecification.classLoader.loadClass('java.util.function.Consumer')
            IS_CONSUMER_CLASS_AVAILABLE = true;
        } catch (ClassNotFoundException ignored) {
            IS_CONSUMER_CLASS_AVAILABLE = false;
        }
    }

    static namespace = new MongoNamespace('databaseName', 'collectionName')
    static codecRegistry = MongoClientSettings.getDefaultCodecRegistry()
    static readPreference = secondary()
    static readConcern = ReadConcern.MAJORITY
    static writeConcern = WriteConcern.MAJORITY
    static filter = new BsonDocument('x', BsonBoolean.TRUE)
    static pipeline = Collections.emptyList()

    @IgnoreIf({ !IS_CONSUMER_CLASS_AVAILABLE })
    def 'should properly override Iterable forEach of Consumer'() {
        given:
        def cannedResults = [new Document('_id', 1), new Document('_id', 2), new Document('_id', 3)]
        def isClosed = false
        def count = 0
        def accepted = 0

        def cursor = {
            Stub(BatchCursor) {
                def results;
                def getResult = {
                    count++
                    results = count == 1 ? cannedResults : null
                    results
                }
                next() >> {
                    getResult()
                }
                hasNext() >> {
                    count == 0
                }
                close() >> {
                    isClosed = true
                }
            }
        }

        when:
        count = 0
        accepted = 0
        def mongoIterable = mongoIterableFactory(new TestOperationExecutor([cursor(), cursor(), cursor(), cursor()]))
        mongoIterable.forEach(new Consumer<Document>() {
            @Override
            void accept(Document document) {
                accepted++
            }
        })

        then:
        accepted == 3
        isClosed

        when:
        count = 0
        accepted = 0
        mongoIterable = mongoIterableFactory(new TestOperationExecutor([cursor(), cursor(), cursor(), cursor()]))
        mongoIterable.forEach(new Consumer<Document>() {
            @Override
            void accept(Document document) {
                accepted++
                throw new MongoInternalException("I don't accept this")
            }
        })

        then:
        thrown(MongoInternalException)
        accepted == 1
        isClosed

        where:
        mongoIterableFactory << [
                { executor ->
                    new Java8FindIterableImpl(null, namespace, Document, Document, codecRegistry,
                            readPreference, readConcern, executor, filter, true)
                },
                { executor ->
                    new Java8AggregateIterableImpl(null, namespace, Document, Document, codecRegistry,
                            readPreference, readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION, true)
                },
                { executor ->
                    new Java8ChangeStreamIterableImpl<>(null, namespace, codecRegistry, readPreference, readConcern, executor, pipeline,
                        Document, ChangeStreamLevel.COLLECTION, true)
                },
                { executor ->
                    new Java8DistinctIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern,
                            executor, 'f1', filter, true)
                },
                { executor ->
                    new Java8ListDatabasesIterableImpl(null, Document, codecRegistry, readPreference,
                            executor, true)
                },
                { executor ->
                    new Java8ListCollectionsIterableImpl(null, 'test', true, Document,
                            codecRegistry, readPreference, executor, true)
                },
                { executor ->
                    new Java8ListIndexesIterableImpl(null, namespace, Document, codecRegistry,
                            readPreference, executor, true)
                },
                { executor ->
                    new Java8MapReduceIterableImpl(null, namespace, Document, BsonDocument,
                            codecRegistry, readPreference, readConcern, writeConcern, executor, 'map', 'reduce')
                }
        ]
    }
}
