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
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.client.ClientSession
import com.mongodb.internal.client.model.AggregationLevel
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.Document
import spock.lang.IgnoreIf
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static spock.util.matcher.HamcrestSupport.expect

class MongoIterablesSpecification extends Specification {

    def executor = new TestOperationExecutor([])
    def clientSession = Stub(ClientSession)
    def namespace = new MongoNamespace('databaseName', 'collectionName')
    def codecRegistry = MongoClientSettings.getDefaultCodecRegistry()
    def readPreference = secondary()
    def readConcern = ReadConcern.MAJORITY
    def writeConcern = WriteConcern.MAJORITY
    def filter = new BsonDocument('x', BsonBoolean.TRUE)
    def pipeline = Collections.emptyList()

    @IgnoreIf({ !Java8MongoIterablesSpecification.IS_CONSUMER_CLASS_AVAILABLE })
    def 'should create Java 8 iterables when java.util.function.Consumer is available'() {
        when:
        def findIterable = MongoIterables.findOf(clientSession, namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, executor, filter, true)

        then:
        expect findIterable, isTheSameAs(new Java8FindIterableImpl<Document, BsonDocument>(clientSession, namespace, Document,
                BsonDocument, codecRegistry, readPreference, readConcern, executor, filter, true))

        when:
        def aggregateIterable = MongoIterables.aggregateOf(clientSession, namespace, Document, BsonDocument, codecRegistry,
                readPreference, readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION, true)

        then:
        expect aggregateIterable, isTheSameAs(new Java8AggregateIterableImpl<Document, BsonDocument>(clientSession, namespace,
                Document, BsonDocument, codecRegistry, readPreference, readConcern, writeConcern, executor, pipeline,
                AggregationLevel.COLLECTION, true))

        when:
        aggregateIterable = MongoIterables.aggregateOf(clientSession, namespace.databaseName, Document, BsonDocument, codecRegistry,
                readPreference, readConcern, writeConcern, executor, pipeline, AggregationLevel.DATABASE, true)

        then:
        expect aggregateIterable, isTheSameAs(new Java8AggregateIterableImpl<Document, BsonDocument>(clientSession, namespace.databaseName,
                Document, BsonDocument, codecRegistry, readPreference, readConcern, writeConcern, executor, pipeline,
                AggregationLevel.DATABASE, true))

        when:
        def changeStreamIterable = MongoIterables.changeStreamOf(clientSession, namespace, codecRegistry,
                readPreference, readConcern, executor, pipeline, Document, ChangeStreamLevel.COLLECTION, true)

        then:
        expect changeStreamIterable, isTheSameAs(new Java8ChangeStreamIterableImpl(clientSession, namespace, codecRegistry,
                readPreference, readConcern, executor, pipeline, Document, ChangeStreamLevel.COLLECTION, true), ['codec'])

        when:
        changeStreamIterable = MongoIterables.changeStreamOf(clientSession, namespace.databaseName, codecRegistry,
                readPreference, readConcern, executor, pipeline, Document, ChangeStreamLevel.COLLECTION, true)

        then:
        expect changeStreamIterable, isTheSameAs(new Java8ChangeStreamIterableImpl(clientSession, namespace.databaseName, codecRegistry,
                readPreference, readConcern, executor, pipeline, Document, ChangeStreamLevel.COLLECTION, true), ['codec'])

        when:
        def distinctIterable = MongoIterables.distinctOf(clientSession, namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, executor, 'f1', filter, true)

        then:
        expect distinctIterable, isTheSameAs(new Java8DistinctIterableImpl(clientSession, namespace, Document, BsonDocument, codecRegistry,
                readPreference, readConcern, executor, 'f1', filter, true))

        when:
        def listDatabasesIterable = MongoIterables.listDatabasesOf(clientSession, Document, codecRegistry, readPreference, executor, true)

        then:
        expect listDatabasesIterable, isTheSameAs(new Java8ListDatabasesIterableImpl(clientSession, Document, codecRegistry, readPreference,
                executor, true))

        when:
        def listCollectionsIterable = MongoIterables.listCollectionsOf(clientSession, 'test', true, Document,
                codecRegistry, readPreference, executor, true)

        then:
        expect listCollectionsIterable, isTheSameAs(new Java8ListCollectionsIterableImpl(clientSession, 'test', true, Document,
                codecRegistry, readPreference, executor, true))

        when:
        def listIndexesIterable = MongoIterables.listIndexesOf(clientSession, namespace, Document, codecRegistry, readPreference, executor,
                true)

        then:
        expect listIndexesIterable, isTheSameAs(new Java8ListIndexesIterableImpl(clientSession, namespace, Document, codecRegistry,
                readPreference, executor, true))

        when:
        def mapReduceIterable = MongoIterables.mapReduceOf(clientSession, namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, writeConcern, executor, 'map', 'reduce')

        then:
        expect mapReduceIterable, isTheSameAs(new Java8MapReduceIterableImpl(clientSession, namespace, Document, BsonDocument,
                codecRegistry, readPreference, readConcern, writeConcern, executor, 'map', 'reduce'))
    }

    @IgnoreIf({ Java8MongoIterablesSpecification.IS_CONSUMER_CLASS_AVAILABLE })
    def 'should create non-Java 8 iterables when java.util.function.Consumer is unavailable'() {
        when:
        def findIterable = MongoIterables.findOf(clientSession, namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, executor, filter, true)

        then:
        expect findIterable, isTheSameAs(new FindIterableImpl<Document, BsonDocument>(clientSession, namespace, Document,
                BsonDocument, codecRegistry, readPreference, readConcern, executor, filter))

        when:
        def aggregateIterable = MongoIterables.aggregateOf(clientSession, namespace, Document, BsonDocument, codecRegistry,
                readPreference, readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION, true)

        then:
        expect aggregateIterable, isTheSameAs(new AggregateIterableImpl<Document, BsonDocument>(clientSession, namespace,
                Document, BsonDocument, codecRegistry, readPreference, readConcern, writeConcern, executor, pipeline,
                AggregationLevel.COLLECTION, true))

        when:
        aggregateIterable = MongoIterables.aggregateOf(clientSession, namespace.databaseName, Document, BsonDocument, codecRegistry,
                readPreference, readConcern, writeConcern, executor, pipeline, AggregationLevel.DATABASE, true)

        then:
        expect aggregateIterable, isTheSameAs(new AggregateIterableImpl<Document, BsonDocument>(clientSession, namespace.databaseName,
                Document, BsonDocument, codecRegistry, readPreference, readConcern, writeConcern, executor, pipeline,
                AggregationLevel.DATABASE))

        when:
        def changeStreamIterable = MongoIterables.changeStreamOf(clientSession, namespace, codecRegistry,
                readPreference, readConcern, executor, pipeline, Document, ChangeStreamLevel.COLLECTION, true)

        then:
        expect changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl(clientSession, namespace, codecRegistry,
                readPreference, readConcern, executor, pipeline, Document, ChangeStreamLevel.COLLECTION, true), ['codec'])

        when:
        changeStreamIterable = MongoIterables.changeStreamOf(clientSession, namespace.databaseName, codecRegistry,
                readPreference, readConcern, executor, pipeline, Document, ChangeStreamLevel.COLLECTION, true)

        then:
        expect changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl(clientSession, namespace.databaseName, codecRegistry,
                readPreference, readConcern, executor, pipeline, Document, ChangeStreamLevel.COLLECTION, true), ['codec'])

        when:
        def distinctIterable = MongoIterables.distinctOf(clientSession, namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, executor, 'f1', filter, true)

        then:
        expect distinctIterable, isTheSameAs(new DistinctIterableImpl(clientSession, namespace, Document, BsonDocument, codecRegistry,
                readPreference, readConcern, executor, 'f1', filter))

        when:
        def listDatabasesIterable = MongoIterables.listDatabasesOf(clientSession, Document, codecRegistry, readPreference, executor, true)

        then:
        expect listDatabasesIterable, isTheSameAs(new ListDatabasesIterableImpl(clientSession, Document, codecRegistry, readPreference,
                executor))

        when:
        def listCollectionsIterable = MongoIterables.listCollectionsOf(clientSession, 'test', true, Document,
                codecRegistry, readPreference, executor, true)

        then:
        expect listCollectionsIterable, isTheSameAs(new ListCollectionsIterableImpl(clientSession, 'test', true, Document,
                codecRegistry, readPreference, executor))

        when:
        def mapReduceIterable = MongoIterables.mapReduceOf(clientSession, namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, writeConcern, executor, 'map', 'reduce')

        then:
        expect mapReduceIterable, isTheSameAs(new MapReduceIterableImpl(clientSession, namespace, Document, BsonDocument,
                codecRegistry, readPreference, readConcern, writeConcern, executor, 'map', 'reduce'))    }
}
