/*
 * Copyright 2014 MongoDB, Inc.
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

package com.mongodb.reactivestreams.client.internal

import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.async.client.AggregateIterable
import com.mongodb.async.client.ChangeStreamIterable
import com.mongodb.async.client.MongoCollection as WrappedMongoCollection
import com.mongodb.async.client.MongoDatabase as WrappedMongoDatabase
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.CreateViewOptions
import com.mongodb.reactivestreams.client.MongoDatabase
import com.mongodb.reactivestreams.client.ClientSession
import com.mongodb.async.client.ClientSession as WrappedClientSession
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.reactivestreams.Subscriber
import spock.lang.Specification

import static com.mongodb.reactivestreams.client.CustomMatchers.isTheSameAs
import static spock.util.matcher.HamcrestSupport.expect

class MongoDatabaseImplSpecification extends Specification {

    def wrappedClientSession = Stub(WrappedClientSession)
    def clientSession = Stub(ClientSession) {
        getWrapped() >> wrappedClientSession
    }

    def 'should have the same methods as the wrapped MongoDatabase'() {
        given:
        def wrapped = WrappedMongoDatabase.methods*.name.sort()
        def local = MongoDatabase.methods*.name.sort()

        expect:
        wrapped == local
    }

    def 'should return the a collection'() {
        given:
        def wrappedCollection = Mock(WrappedMongoCollection)
        def wrapped = Mock(WrappedMongoDatabase) {
            getCollection(_) >> wrappedCollection
            getCollection(_, _) >> wrappedCollection
        }
        def mongoDatabase = new MongoDatabaseImpl(wrapped)

        when:
        def collection = mongoDatabase.getCollection('collectionName')

        then:
        expect collection, isTheSameAs(new MongoCollectionImpl(wrappedCollection))

        when:
        collection = mongoDatabase.getCollection('collectionName', Document)

        then:
        expect collection, isTheSameAs(new MongoCollectionImpl(wrappedCollection))
    }

    def 'should call the underlying getName'() {
        given:
        def wrapped = Mock(WrappedMongoDatabase)
        def mongoDatabase = new MongoDatabaseImpl(wrapped)

        when:
        mongoDatabase.getName()

        then:
        1 * wrapped.getName()
    }

    def 'should call the underlying getCodecRegistry'() {
        given:
        def wrapped = Mock(WrappedMongoDatabase)
        def mongoDatabase = new MongoDatabaseImpl(wrapped)

        when:
        mongoDatabase.getCodecRegistry()

        then:
        1 * wrapped.getCodecRegistry()
    }

    def 'should call the underlying getReadPreference'() {
        given:
        def wrapped = Mock(WrappedMongoDatabase)
        def mongoDatabase = new MongoDatabaseImpl(wrapped)

        when:
        mongoDatabase.getReadPreference()

        then:
        1 * wrapped.getReadPreference()

    }

    def 'should call the underlying getWriteConcern'() {
        given:
        def wrapped = Mock(WrappedMongoDatabase)
        def mongoDatabase = new MongoDatabaseImpl(wrapped)

        when:
        mongoDatabase.getWriteConcern()

        then:
        1 * wrapped.getWriteConcern()
    }

    def 'should call the underlying getReadConcern'() {
        given:
        def wrapped = Mock(WrappedMongoDatabase)
        def mongoDatabase = new MongoDatabaseImpl(wrapped)

        when:
        mongoDatabase.getReadConcern()

        then:
        1 * wrapped.getReadConcern()
    }

    def 'should call the underlying withCodecRegistry'() {
        given:
        def codecRegistry = Stub(CodecRegistry)
        def wrappedResult = Stub(WrappedMongoDatabase)
        def wrapped = Mock(WrappedMongoDatabase) {
            1 * withCodecRegistry(codecRegistry) >> wrappedResult
        }
        def mongoDatabase = new MongoDatabaseImpl(wrapped)

        when:
        def result = mongoDatabase.withCodecRegistry(codecRegistry)

        then:
        expect result, isTheSameAs(new MongoDatabaseImpl(wrappedResult))
    }

    def 'should call the underlying withReadPreference'() {
        given:
        def readPreference = Stub(ReadPreference)
        def wrappedResult = Stub(WrappedMongoDatabase)
        def wrapped = Mock(WrappedMongoDatabase) {
            1 * withReadPreference(readPreference) >> wrappedResult
        }
        def mongoDatabase = new MongoDatabaseImpl(wrapped)

        when:
        def result = mongoDatabase.withReadPreference(readPreference)

        then:
        expect result, isTheSameAs(new MongoDatabaseImpl(wrappedResult))
    }

    def 'should call the underlying withWriteConcern'() {
        given:
        def writeConcern = Stub(WriteConcern)
        def wrappedResult = Stub(WrappedMongoDatabase)
        def wrapped = Mock(WrappedMongoDatabase) {
            1 * withWriteConcern(writeConcern) >> wrappedResult
        }
        def mongoDatabase = new MongoDatabaseImpl(wrapped)

        when:
        def result = mongoDatabase.withWriteConcern(writeConcern)

        then:
        expect result, isTheSameAs(new MongoDatabaseImpl(wrappedResult))
    }


    def 'should call the underlying withReadConcern'() {
        given:
        def readConcern = ReadConcern.DEFAULT
        def wrappedResult = Stub(WrappedMongoDatabase)
        def wrapped = Mock(WrappedMongoDatabase) {
            1 * withReadConcern(readConcern) >> wrappedResult
        }
        def mongoDatabase = new MongoDatabaseImpl(wrapped)

        when:
        def result = mongoDatabase.withReadConcern(readConcern)

        then:
        expect result, isTheSameAs(new MongoDatabaseImpl(wrappedResult))
    }

    def 'should call the underlying runCommand when writing'() {
        given:
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(1) }
        }
        def wrapped = Mock(WrappedMongoDatabase)
        def mongoDatabase = new MongoDatabaseImpl(wrapped)

        when:
        mongoDatabase.runCommand(new Document())

        then: 'only executed when requested'
        0 * wrapped.runCommand(_, _, _)

        when:
        mongoDatabase.runCommand(new Document()).subscribe(subscriber)

        then:
        1 * wrapped.runCommand(new Document(), Document, _)

        when:
        mongoDatabase.runCommand(new BsonDocument(), BsonDocument).subscribe(subscriber)

        then:
        1 * wrapped.runCommand(new BsonDocument(), BsonDocument, _)

        when:
        mongoDatabase.runCommand(clientSession, new Document()).subscribe(subscriber)

        then:
        1 * wrapped.runCommand(wrappedClientSession, new Document(), Document, _)

        when:
        mongoDatabase.runCommand(clientSession, new BsonDocument(), BsonDocument).subscribe(subscriber)

        then:
        1 * wrapped.runCommand(wrappedClientSession, new BsonDocument(), BsonDocument, _)
    }

    def 'should call the underlying runCommand for read operations'() {
        given:
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(1) }
        }
        def readPreference = Stub(ReadPreference)
        def wrapped = Mock(WrappedMongoDatabase)
        def mongoDatabase = new MongoDatabaseImpl(wrapped)

        when:
        mongoDatabase.runCommand(new Document(), readPreference)

        then: 'only executed when requested'
        0 * wrapped.runCommand(_, _, _, _)

        when:
        mongoDatabase.runCommand(new Document(), readPreference).subscribe(subscriber)

        then:
        1 * wrapped.runCommand(new Document(), readPreference, Document, _)

        when:
        mongoDatabase.runCommand(new BsonDocument(), readPreference, BsonDocument).subscribe(subscriber)

        then:
        1 * wrapped.runCommand(new BsonDocument(), readPreference, BsonDocument, _)

        when:
        mongoDatabase.runCommand(clientSession, new Document(), readPreference).subscribe(subscriber)

        then:
        1 * wrapped.runCommand(wrappedClientSession, new Document(), readPreference, Document, _)

        when:
        mongoDatabase.runCommand(clientSession, new BsonDocument(), readPreference, BsonDocument).subscribe(subscriber)

        then:
        1 * wrapped.runCommand(wrappedClientSession, new BsonDocument(), readPreference, BsonDocument, _)
    }

    def 'should call the underlying drop'() {
        given:
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(1) }
        }
        def wrapped = Mock(WrappedMongoDatabase)
        def mongoDatabase = new MongoDatabaseImpl(wrapped)

        when:
        mongoDatabase.drop()

        then: 'only executed when requested'
        0 * wrapped.drop(_)

        when:
        mongoDatabase.drop().subscribe(subscriber)

        then:
        1 * wrapped.drop(_)


        when:
        mongoDatabase.drop(clientSession).subscribe(subscriber)

        then:
        1 * wrapped.drop(wrappedClientSession, _)
    }

    def 'should call the underlying listCollectionNames'() {
        given:
        def wrapped = Mock(WrappedMongoDatabase)
        def mongoDatabase = new MongoDatabaseImpl(wrapped)

        when:
        mongoDatabase.listCollectionNames()

        then:
        1 * wrapped.listCollectionNames()

        when:
        mongoDatabase.listCollectionNames(clientSession)

        then:
        1 * wrapped.listCollectionNames(wrappedClientSession)

    }

    def 'should call the underlying listCollections'() {
        given:
        def wrappedResult = Stub(com.mongodb.async.client.ListCollectionsIterable)
        def wrapped = Mock(WrappedMongoDatabase)
        def mongoDatabase = new MongoDatabaseImpl(wrapped)

        when:
        def publisher = mongoDatabase.listCollections()

        then:
        1 * wrapped.listCollections(Document) >> wrappedResult
        expect publisher, isTheSameAs(new ListCollectionsPublisherImpl(wrappedResult))

        when:
        publisher = mongoDatabase.listCollections(BsonDocument)

        then:
        1 * wrapped.listCollections(BsonDocument) >> wrappedResult
        expect publisher, isTheSameAs(new ListCollectionsPublisherImpl(wrappedResult))

        when:
        publisher = mongoDatabase.listCollections(clientSession)

        then:
        1 * wrapped.listCollections(wrappedClientSession, Document) >> wrappedResult
        expect publisher, isTheSameAs(new ListCollectionsPublisherImpl(wrappedResult))

        when:
        publisher = mongoDatabase.listCollections(clientSession, BsonDocument)

        then:
        1 * wrapped.listCollections(wrappedClientSession, BsonDocument) >> wrappedResult
        expect publisher, isTheSameAs(new ListCollectionsPublisherImpl(wrappedResult))
    }

    def 'should call the underlying createCollection'() {
        given:
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(1) }
        }
        def createCollectionOptions = Stub(CreateCollectionOptions)
        def wrapped = Mock(WrappedMongoDatabase)
        def mongoDatabase = new MongoDatabaseImpl(wrapped)

        when:
        mongoDatabase.createCollection('collectionName')

        then: 'only executed when requested'
        0 * wrapped.createCollection(_, _, _)

        when:
        mongoDatabase.createCollection('collectionName').subscribe(subscriber)

        then:
        1 * wrapped.createCollection('collectionName', _, _)

        when:
        mongoDatabase.createCollection('collectionName', createCollectionOptions).subscribe(subscriber)

        then:
        1 * wrapped.createCollection('collectionName', createCollectionOptions, _)

        when:
        mongoDatabase.createCollection(clientSession, 'collectionName').subscribe(subscriber)

        then:
        1 * wrapped.createCollection(wrappedClientSession, 'collectionName', _, _)

        when:
        mongoDatabase.createCollection(clientSession, 'collectionName', createCollectionOptions).subscribe(subscriber)

        then:
        1 * wrapped.createCollection(wrappedClientSession, 'collectionName', createCollectionOptions, _)
    }

    def 'should call the underlying createView'() {
        given:
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(1) }
        }
        def createViewOptions = Stub(CreateViewOptions)
        def wrapped = Mock(WrappedMongoDatabase)
        def mongoDatabase = new MongoDatabaseImpl(wrapped)
        def viewName = 'view1'
        def viewOn = 'col1'
        def pipeline = [new Document('$match', new Document('x', true))]

        when:
        mongoDatabase.createView(viewName, viewOn, pipeline)

        then: 'only executed when requested'
        0 * wrapped.createView(_, _, _, _, _)

        when:
        mongoDatabase.createView(viewName, viewOn, pipeline).subscribe(subscriber)

        then:
        1 * wrapped.createView(viewName, viewOn, pipeline, _, _)

        when:
        mongoDatabase.createView(viewName, viewOn, pipeline, createViewOptions).subscribe(subscriber)

        then:
        1 * wrapped.createView(viewName, viewOn, pipeline, createViewOptions, _)

        when:
        mongoDatabase.createView(clientSession, viewName, viewOn, pipeline).subscribe(subscriber)

        then:
        1 * wrapped.createView(wrappedClientSession, viewName, viewOn, pipeline, _, _)

        when:
        mongoDatabase.createView(clientSession, viewName, viewOn, pipeline, createViewOptions).subscribe(subscriber)

        then:
        1 * wrapped.createView(wrappedClientSession, viewName, viewOn, pipeline, createViewOptions, _)
    }

    def 'should use ChangeStreamPublisher correctly'() {
        given:
        def pipeline = [new Document('$match', 1)]
        def wrappedResult = Stub(ChangeStreamIterable)
        def wrapped = Mock(WrappedMongoDatabase)
        def mongoDatabase = new MongoDatabaseImpl(wrapped)
        def changeStreamPublisher

        when:
        changeStreamPublisher = mongoDatabase.watch()

        then:
        1 * wrapped.watch([], Document) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = mongoDatabase.watch(BsonDocument)

        then:
        1 * wrapped.watch([], BsonDocument) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = mongoDatabase.watch(pipeline)

        then:
        1 * wrapped.watch(pipeline, Document) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = mongoDatabase.watch(pipeline, BsonDocument)

        then:
        1 * wrapped.watch(pipeline, BsonDocument) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = mongoDatabase.watch(clientSession)

        then:
        1 * wrapped.watch(wrappedClientSession, [], Document) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = mongoDatabase.watch(clientSession, BsonDocument)

        then:
        1 * wrapped.watch(wrappedClientSession, [], BsonDocument) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = mongoDatabase.watch(clientSession, pipeline)

        then:
        1 * wrapped.watch(wrappedClientSession, pipeline, Document) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = mongoDatabase.watch(clientSession, pipeline, BsonDocument)

        then:
        1 * wrapped.watch(wrappedClientSession, pipeline, BsonDocument) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))
    }

    def 'should use AggregatePublisher correctly'() {
        given:
        def pipeline = [new Document('$match', 1)]
        def wrappedResult = Stub(AggregateIterable)
        def wrapped = Mock(WrappedMongoDatabase)
        def mongoDatabase = new MongoDatabaseImpl(wrapped)
        def aggregatePublisher

        when:
        aggregatePublisher = mongoDatabase.aggregate(pipeline)

        then:
        1 * wrapped.aggregate(pipeline, Document) >> wrappedResult
        expect aggregatePublisher, isTheSameAs(new AggregatePublisherImpl(wrappedResult))

        when:
        aggregatePublisher = mongoDatabase.aggregate(pipeline, BsonDocument)

        then:
        1 * wrapped.aggregate(pipeline, BsonDocument) >> wrappedResult
        expect aggregatePublisher, isTheSameAs(new AggregatePublisherImpl(wrappedResult))

        when:
        aggregatePublisher = mongoDatabase.aggregate(clientSession, pipeline)

        then:
        1 * wrapped.aggregate(wrappedClientSession, pipeline, Document) >> wrappedResult
        expect aggregatePublisher, isTheSameAs(new AggregatePublisherImpl(wrappedResult))

        when:
        aggregatePublisher = mongoDatabase.aggregate(clientSession, pipeline, BsonDocument)

        then:
        1 * wrapped.aggregate(wrappedClientSession, pipeline, BsonDocument) >> wrappedResult
        expect aggregatePublisher, isTheSameAs(new AggregatePublisherImpl(wrappedResult))
    }
}
