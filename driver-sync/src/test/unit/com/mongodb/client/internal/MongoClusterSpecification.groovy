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

package com.mongodb.client.internal

import com.mongodb.MongoClientSettings
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.client.ClientSession
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoIterable
import com.mongodb.internal.TimeoutSettings
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel
import com.mongodb.internal.connection.Cluster
import com.mongodb.internal.session.ServerSessionPool
import com.mongodb.internal.tracing.TracingManager
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.UuidCodec
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecRegistry
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.client.internal.TestHelper.execute
import static org.bson.UuidRepresentation.UNSPECIFIED
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class MongoClusterSpecification extends Specification {

    private static final CodecRegistry CODEC_REGISTRY = fromProviders(new ValueCodecProvider())
    private static final MongoClientSettings CLIENT_SETTINGS = MongoClientSettings.builder().build()
    private static final TimeoutSettings TIMEOUT_SETTINGS = TimeoutSettings.create(CLIENT_SETTINGS)
    private final Cluster cluster = Stub(Cluster)
    private final MongoClient originator =  Stub(MongoClient)
    private final ServerSessionPool serverSessionPool = Stub(ServerSessionPool)
    private final OperationExecutor operationExecutor = Stub(OperationExecutor)

    def 'should pass the correct settings to getDatabase'() {
        given:
        def settings = MongoClientSettings.builder()
                .readPreference(secondary())
                .writeConcern(WriteConcern.MAJORITY)
                .readConcern(ReadConcern.MAJORITY)
                .retryWrites(true)
                .codecRegistry(CODEC_REGISTRY)
                .build()
        def operationExecutor = new TestOperationExecutor([])
        def mongoClientCluster = createMongoCluster(settings, operationExecutor)

        when:
        def database = mongoClientCluster.getDatabase('name')

        then:
        expect database, isTheSameAs(expectedDatabase)

        where:
        expectedDatabase << new MongoDatabaseImpl('name', CODEC_REGISTRY, secondary(),
                WriteConcern.MAJORITY, true, true, ReadConcern.MAJORITY, UNSPECIFIED, null,
                TIMEOUT_SETTINGS, new TestOperationExecutor([]))
    }

    def 'should behave correctly when using withCodecRegistry'() {
        given:
        def newCodecRegistry = fromProviders(new ValueCodecProvider())

        when:
        def mongoCluster = createMongoCluster().withCodecRegistry(newCodecRegistry)

        then:
        (mongoCluster.getCodecRegistry().get(UUID) as UuidCodec).getUuidRepresentation() == UNSPECIFIED
        expect mongoCluster, isTheSameAs(createMongoCluster(
                MongoClientSettings.builder(CLIENT_SETTINGS).codecRegistry(newCodecRegistry).build()))
    }

    def 'should behave correctly when using withReadPreference'() {
        given:
        def newReadPreference = ReadPreference.secondaryPreferred()

        when:
        def mongoCluster =  createMongoCluster().withReadPreference(newReadPreference)

        then:
        mongoCluster.getReadPreference() == newReadPreference
        expect mongoCluster, isTheSameAs(
                createMongoCluster(MongoClientSettings.builder(CLIENT_SETTINGS).readPreference(newReadPreference).build()))
    }

    def 'should behave correctly when using withWriteConcern'() {
        given:
        def newWriteConcern = WriteConcern.MAJORITY

        when:
        def mongoCluster =  createMongoCluster().withWriteConcern(newWriteConcern)

        then:
        mongoCluster.getWriteConcern() == newWriteConcern
        expect mongoCluster, isTheSameAs(createMongoCluster(
                MongoClientSettings.builder(CLIENT_SETTINGS).writeConcern(newWriteConcern).build()))
    }

    def 'should behave correctly when using withReadConcern'() {
        given:
        def newReadConcern = ReadConcern.MAJORITY

        when:
        def mongoCluster =  createMongoCluster().withReadConcern(newReadConcern)

        then:
        mongoCluster.getReadConcern() == newReadConcern
        expect mongoCluster, isTheSameAs(createMongoCluster(
                MongoClientSettings.builder(CLIENT_SETTINGS).readConcern(newReadConcern).build()))
    }

    def 'should behave correctly when using withTimeout'() {
        when:
        def mongoCluster =  createMongoCluster().withTimeout(10_000, TimeUnit.MILLISECONDS)

        then:
        mongoCluster.getTimeout(TimeUnit.MILLISECONDS) == 10_000
        expect mongoCluster, isTheSameAs(createMongoCluster(MongoClientSettings.builder(CLIENT_SETTINGS)
                .timeout(10_000, TimeUnit.MILLISECONDS).build()))

        when:
        createMongoCluster().withTimeout(500, TimeUnit.NANOSECONDS)

        then:
        thrown(IllegalArgumentException)
    }


    def 'should use ListDatabasesIterableImpl correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def mongoCluster = createMongoCluster(executor)
        def listDatabasesMethod = mongoCluster.&listDatabases
        def listDatabasesNamesMethod = mongoCluster.&listDatabaseNames

        when:
        def listDatabasesIterable = execute(listDatabasesMethod, session)

        then:
        expect listDatabasesIterable, isTheSameAs(new ListDatabasesIterableImpl<>(session, Document,
                CLIENT_SETTINGS.codecRegistry, primary(), executor, true, TIMEOUT_SETTINGS))

        when:
        listDatabasesIterable = execute(listDatabasesMethod, session, BsonDocument)

        then:
        expect listDatabasesIterable, isTheSameAs(new ListDatabasesIterableImpl<>(session, BsonDocument,
                CLIENT_SETTINGS.codecRegistry, primary(), executor, true, TIMEOUT_SETTINGS))

        when:
        def listDatabaseNamesIterable = execute(listDatabasesNamesMethod, session) as MongoIterable<String>

        then:
        // listDatabaseNamesIterable is an instance of a MappingIterable, so have to get the mapped iterable inside it
        expect listDatabaseNamesIterable.getMapped(), isTheSameAs(new ListDatabasesIterableImpl<>(session, BsonDocument,
                CLIENT_SETTINGS.codecRegistry, primary(), executor, true, TIMEOUT_SETTINGS)
                .nameOnly(true))

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should create ChangeStreamIterable correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def namespace = new MongoNamespace('admin', '_ignored')
        def settings = MongoClientSettings.builder()
                .readPreference(secondary())
                .readConcern(ReadConcern.MAJORITY)
                .codecRegistry(getDefaultCodecRegistry())
                .build()
        def readPreference = settings.getReadPreference()
        def readConcern = settings.getReadConcern()
        def mongoCluster = createMongoCluster(settings, executor)
        def watchMethod = mongoCluster.&watch

        when:
        def changeStreamIterable = execute(watchMethod, session)

        then:
        expect changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl<>(session, namespace, settings.codecRegistry,
                readPreference, readConcern, executor, [], Document, ChangeStreamLevel.CLIENT, true, TIMEOUT_SETTINGS),
                ['codec'])

        when:
        changeStreamIterable = execute(watchMethod, session, [new Document('$match', 1)])

        then:
        expect changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl<>(session, namespace, settings.codecRegistry,
                readPreference, readConcern, executor, [new Document('$match', 1)], Document, ChangeStreamLevel.CLIENT,
                true, TIMEOUT_SETTINGS), ['codec'])

        when:
        changeStreamIterable = execute(watchMethod, session, [new Document('$match', 1)], BsonDocument)

        then:
        expect changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl<>(session, namespace, settings.codecRegistry,
                readPreference, readConcern, executor, [new Document('$match', 1)], BsonDocument,
                ChangeStreamLevel.CLIENT, true, TIMEOUT_SETTINGS), ['codec'])

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should validate the ChangeStreamIterable pipeline data correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def mongoCluster = createMongoCluster(executor)

        when:
        mongoCluster.watch((Class) null)

        then:
        thrown(IllegalArgumentException)

        when:
        mongoCluster.watch([null]).into([])

        then:
        thrown(IllegalArgumentException)
    }

    MongoClusterImpl createMongoCluster() {
        createMongoCluster(CLIENT_SETTINGS)
    }

    MongoClusterImpl createMongoCluster(final MongoClientSettings settings) {
        createMongoCluster(settings, operationExecutor)
    }

    MongoClusterImpl createMongoCluster(final OperationExecutor operationExecutor) {
        createMongoCluster(CLIENT_SETTINGS, operationExecutor)
    }

    MongoClusterImpl createMongoCluster(final MongoClientSettings settings, final OperationExecutor operationExecutor) {
        new MongoClusterImpl(null, cluster, settings.codecRegistry, null, null,
                originator, operationExecutor, settings.readConcern, settings.readPreference, settings.retryReads, settings.retryWrites,
                null, serverSessionPool, TimeoutSettings.create(settings), settings.uuidRepresentation,
                settings.writeConcern, TracingManager.NO_OP)
    }
}
