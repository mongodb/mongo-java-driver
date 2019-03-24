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

package com.mongodb

import com.mongodb.client.ClientSession
import com.mongodb.client.internal.MongoClientImpl
import com.mongodb.client.internal.MongoDatabaseImpl
import com.mongodb.client.internal.MongoIterables
import com.mongodb.client.internal.TestOperationExecutor
import com.mongodb.client.model.geojson.MultiPolygon
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel
import com.mongodb.internal.connection.Cluster
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecRegistry
import org.bson.internal.OverridableUuidRepresentationCodecRegistry
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.client.internal.TestHelper.execute
import static org.bson.UuidRepresentation.STANDARD
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class MongoClientSpecification extends Specification {

    private static CodecRegistry codecRegistry = fromProviders(new ValueCodecProvider())

    def 'default codec registry should contain all supported providers'() {
        given:
        def codecRegistry = getDefaultCodecRegistry()

        expect:
        codecRegistry.get(BsonDocument)
        codecRegistry.get(BasicDBObject)
        codecRegistry.get(Document)
        codecRegistry.get(Integer)
        codecRegistry.get(MultiPolygon)
        codecRegistry.get(Iterable)
    }

    def 'should use ListDatabasesIterableImpl correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def client = Spy(MongoClient) {
            3 * createOperationExecutor() >> {
                executor
            }
        }
        def listDatabasesMethod = client.&listDatabases
        def listDatabasesNamesMethod = client.&listDatabaseNames

        when:
        def listDatabasesIterable = execute(listDatabasesMethod, session)

        then:
        expect listDatabasesIterable, isTheSameAs(MongoIterables.listDatabasesOf(session, Document, getDefaultCodecRegistry(),
                primary(), executor, true))

        when:
        listDatabasesIterable = execute(listDatabasesMethod, session, BsonDocument)

        then:
        expect listDatabasesIterable, isTheSameAs(MongoIterables.listDatabasesOf(session, BsonDocument,
                getDefaultCodecRegistry(), primary(), executor, true))

        when:
        def listDatabaseNamesIterable = execute(listDatabasesNamesMethod, session)

        then:
        // listDatabaseNamesIterable is an instance of a MappingIterable, so have to get the mapped iterable inside it
        expect listDatabaseNamesIterable.getMapped(), isTheSameAs(MongoIterables.listDatabasesOf(session, BsonDocument,
                getDefaultCodecRegistry(), primary(), executor, true).nameOnly(true))

        cleanup:
        client?.close()

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should create ChangeStreamIterable correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def namespace = new MongoNamespace('admin', 'ignored')
        def settings = MongoClientOptions.builder().build()
        def codecRegistry = settings.getCodecRegistry()
        def readPreference = settings.getReadPreference()
        def readConcern = settings.getReadConcern()

        def client = Spy(MongoClient) {
            3 * createOperationExecutor() >> {
                executor
            }
        }
        def watchMethod = client.&watch

        when:
        def changeStreamIterable = execute(watchMethod, session)

        then:
        expect changeStreamIterable, isTheSameAs(MongoIterables.changeStreamOf(session, namespace, codecRegistry,
                readPreference,
                readConcern, executor, [], Document, ChangeStreamLevel.CLIENT, true), ['codec'])

        when:
        changeStreamIterable = execute(watchMethod, session, [new Document('$match', 1)])

        then:
        expect changeStreamIterable, isTheSameAs(MongoIterables.changeStreamOf(session, namespace, codecRegistry, readPreference,
                readConcern, executor, [new Document('$match', 1)], Document, ChangeStreamLevel.CLIENT, true), ['codec'])

        when:
        changeStreamIterable = execute(watchMethod, session, [new Document('$match', 1)], BsonDocument)

        then:
        expect changeStreamIterable, isTheSameAs(MongoIterables.changeStreamOf(session, namespace, codecRegistry, readPreference,
                readConcern, executor, [new Document('$match', 1)], BsonDocument, ChangeStreamLevel.CLIENT, true), ['codec'])

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should validate the ChangeStreamIterable pipeline data correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def client = new MongoClientImpl(Stub(Cluster), MongoClientSettings.builder().build(), executor)

        when:
        client.watch((Class) null)

        then:
        thrown(IllegalArgumentException)

        when:
        client.watch([null]).into([])

        then:
        thrown(IllegalArgumentException)
    }
    def 'should pass the correct settings to getDatabase'() {
        given:
        def options = MongoClientOptions.builder()
                .readPreference(secondary())
                .writeConcern(WriteConcern.MAJORITY)
                .readConcern(ReadConcern.MAJORITY)
                .retryWrites(true)
                .codecRegistry(codecRegistry)
                .uuidRepresentation(STANDARD)
                .build()
        def client = new MongoClient('localhost', options)

        when:
        def database = client.getDatabase('name')

        then:
        expect database, isTheSameAs(new MongoDatabaseImpl('name', client.getDelegate().getCodecRegistry(), secondary(),
                WriteConcern.MAJORITY, true, true, ReadConcern.MAJORITY, STANDARD,
                client.getDelegate().getOperationExecutor()))
    }

    def 'should create registry reflecting UuidRepresentation'() {
        given:
        def options = MongoClientOptions.builder()
                .codecRegistry(codecRegistry)
                .uuidRepresentation(STANDARD)
                .build()

        when:
        def client = new MongoClient('localhost', options)
        def registry = client.getCodecRegistry()

        then:
        registry instanceof OverridableUuidRepresentationCodecRegistry
        (registry as OverridableUuidRepresentationCodecRegistry).uuidRepresentation == STANDARD
        (registry as OverridableUuidRepresentationCodecRegistry).wrapped == codecRegistry

        cleanup:
        client?.close()
    }
}
