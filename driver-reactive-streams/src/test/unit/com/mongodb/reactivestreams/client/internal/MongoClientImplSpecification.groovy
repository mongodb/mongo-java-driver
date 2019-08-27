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

package com.mongodb.reactivestreams.client.internal

import com.mongodb.ClientSessionOptions
import com.mongodb.internal.async.client.ChangeStreamIterable
import com.mongodb.internal.async.client.ClientSession as WrappedClientSession
import com.mongodb.internal.async.client.ListDatabasesIterable
import com.mongodb.internal.async.client.MongoClient as WrappedMongoClient
import com.mongodb.reactivestreams.client.ClientSession
import com.mongodb.reactivestreams.client.MongoClient
import org.bson.BsonDocument
import org.bson.Document
import org.reactivestreams.Subscriber
import spock.lang.Specification

import static com.mongodb.reactivestreams.client.CustomMatchers.isTheSameAs
import static spock.util.matcher.HamcrestSupport.expect

class MongoClientImplSpecification extends Specification {

    def wrapped = Mock(WrappedMongoClient)
    def mongoClient = new MongoClientImpl(wrapped)
    def wrappedClientSession = Stub(WrappedClientSession)
    def clientSession = Stub(ClientSession) {
        getWrapped() >> wrappedClientSession
    }

    def 'should have the same methods as the wrapped MongoClient'() {
        given:
        def wrapped = WrappedMongoClient.methods*.name.sort()
        def local = MongoClient.methods*.name.sort()

        expect:
        wrapped == local
    }

    def 'should call the underlying listDatabases'() {
        given:
        def wrappedResult = Stub(ListDatabasesIterable)
        def wrapped = Mock(WrappedMongoClient)
        def mongoClient = new MongoClientImpl(wrapped)

        when:
        def publisher = mongoClient.listDatabases()

        then:
        1 * wrapped.listDatabases(Document) >> wrappedResult
        expect publisher, isTheSameAs(new ListDatabasesPublisherImpl(wrappedResult))

        when:
        publisher = mongoClient.listDatabases(BsonDocument)

        then:
        1 * wrapped.listDatabases(BsonDocument) >> wrappedResult
        expect publisher, isTheSameAs(new ListDatabasesPublisherImpl(wrappedResult))

        when:
        publisher = mongoClient.listDatabases(clientSession)

        then:
        1 * wrapped.listDatabases(wrappedClientSession, Document) >> wrappedResult
        expect publisher, isTheSameAs(new ListDatabasesPublisherImpl(wrappedResult))

        when:
        publisher = mongoClient.listDatabases(clientSession, BsonDocument)

        then:
        1 * wrapped.listDatabases(wrappedClientSession, BsonDocument) >> wrappedResult
        expect publisher, isTheSameAs(new ListDatabasesPublisherImpl(wrappedResult))
    }

    def 'should call the underlying listDatabaseNames'() {
        when:
        mongoClient.listDatabaseNames()

        then:
        1 * wrapped.listDatabaseNames()

        when:
        mongoClient.listDatabaseNames(clientSession)

        then:
        1 * wrapped.listDatabaseNames(wrappedClientSession)
    }


    def 'should use ChangeStreamPublisher correctly'() {
        given:
        def pipeline = [new Document('$match', 1)]
        def wrappedResult = Stub(ChangeStreamIterable)
        def changeStreamPublisher

        when:
        changeStreamPublisher = mongoClient.watch()

        then:
        1 * wrapped.watch([], Document) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = mongoClient.watch(BsonDocument)

        then:
        1 * wrapped.watch([], BsonDocument) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = mongoClient.watch(pipeline)

        then:
        1 * wrapped.watch(pipeline, Document) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = mongoClient.watch(pipeline, BsonDocument)

        then:
        1 * wrapped.watch(pipeline, BsonDocument) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = mongoClient.watch(clientSession)

        then:
        1 * wrapped.watch(wrappedClientSession, [], Document) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = mongoClient.watch(clientSession, BsonDocument)

        then:
        1 * wrapped.watch(wrappedClientSession, [], BsonDocument) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = mongoClient.watch(clientSession, pipeline)

        then:
        1 * wrapped.watch(wrappedClientSession, pipeline, Document) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = mongoClient.watch(clientSession, pipeline, BsonDocument)

        then:
        1 * wrapped.watch(wrappedClientSession, pipeline, BsonDocument) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))
    }

    def 'should call the underlying withSession'() {
        given:
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(1) }
        }
        def wrapped = Mock(WrappedMongoClient)
        def mongoClient = new MongoClientImpl(wrapped)
        def options = ClientSessionOptions.builder().causallyConsistent(false).build()

        when:
        mongoClient.startSession().subscribe(subscriber)

        then:
        1 * wrapped.startSession(ClientSessionOptions.builder().build(), _)

        when:
        mongoClient.startSession(options).subscribe(subscriber)

        then:
        1 * wrapped.startSession(options, _)
    }
}
