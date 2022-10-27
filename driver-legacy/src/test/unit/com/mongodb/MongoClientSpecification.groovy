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

import com.mongodb.client.internal.MongoClientImpl
import com.mongodb.client.internal.MongoDatabaseImpl
import com.mongodb.client.internal.TestOperationExecutor
import com.mongodb.client.model.geojson.MultiPolygon
import com.mongodb.connection.ClusterSettings
import com.mongodb.internal.connection.Cluster
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.UuidCodec
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecRegistry
import org.bson.json.JsonObject
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry
import static com.mongodb.MongoCredential.createMongoX509Credential
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterConnectionMode.SINGLE
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static org.bson.UuidRepresentation.C_SHARP_LEGACY
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
        codecRegistry.get(Collection)
        codecRegistry.get(Iterable)
        codecRegistry.get(JsonObject)
    }

    def 'should construct with correct settings'() {
        expect:
        client.delegate.settings.clusterSettings == clusterSettings
        client.credential == credential

        cleanup:
        client?.close()

        where:
        client | clusterSettings | credential
        new MongoClient()             |
                ClusterSettings.builder().build() |
                null
        new MongoClient('host:27018') |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(SINGLE).build() |
                null
        new MongoClient('host', 27018) |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(SINGLE).build() |
                null
        new MongoClient('mongodb://host:27018') |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(SINGLE).build() |
                null
        new MongoClient('mongodb://user:pwd@host:27018') |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(SINGLE).build() |
                MongoCredential.createCredential('user', 'admin', 'pwd'.toCharArray())
        new MongoClient('mongodb+srv://test3.test.build.10gen.cc') |
                ClusterSettings.builder().srvHost('test3.test.build.10gen.cc').mode(MULTIPLE).build() |
                null
        new MongoClient('mongodb+srv://user:pwd@test3.test.build.10gen.cc') |
                ClusterSettings.builder().srvHost('test3.test.build.10gen.cc').mode(MULTIPLE).build() |
                MongoCredential.createCredential('user', 'admin', 'pwd'.toCharArray())
        new MongoClient(new ConnectionString('mongodb://host:27018')) |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(SINGLE).build() |
                null
        new MongoClient(new ConnectionString('mongodb://user:pwd@host:27018')) |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(SINGLE).build() |
                MongoCredential.createCredential('user', 'admin', 'pwd'.toCharArray())
        new MongoClient(new ConnectionString('mongodb+srv://test3.test.build.10gen.cc')) |
                ClusterSettings.builder().srvHost('test3.test.build.10gen.cc').mode(MULTIPLE).build() |
                null
        new MongoClient(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString('mongodb://host:27018')).build()) |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(SINGLE).build() |
                null
        new MongoClient(new MongoClientURI('mongodb://host:27018')) |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(SINGLE).build() |
                null
        new MongoClient(new MongoClientURI('mongodb://host:27018/?replicaSet=rs0')) |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(MULTIPLE).requiredReplicaSetName('rs0')
                        .build() |
                null
        new MongoClient(new MongoClientURI('mongodb://user:pwd@host:27018')) |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(SINGLE).build() |
                MongoCredential.createCredential('user', 'admin', 'pwd'.toCharArray())
        new MongoClient(new MongoClientURI('mongodb://host1:27018,host2:27018')) |
                ClusterSettings.builder().hosts([new ServerAddress('host1:27018'), new ServerAddress('host2:27018')])
                        .mode(MULTIPLE).build() |
                null
        new MongoClient(new MongoClientURI('mongodb+srv://test3.test.build.10gen.cc')) |
                ClusterSettings.builder().srvHost('test3.test.build.10gen.cc').mode(MULTIPLE).build() |
                null
        new MongoClient('host:27018', MongoClientOptions.builder().serverSelectionTimeout(5).build()) |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(SINGLE)
                        .serverSelectionTimeout(5, MILLISECONDS).build() |
                null
        new MongoClient(new ServerAddress('host:27018')) |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(SINGLE).build() |
                null
        new MongoClient(new ServerAddress('host:27018'), MongoClientOptions.builder().serverSelectionTimeout(5).build()) |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(SINGLE)
                        .serverSelectionTimeout(5, MILLISECONDS).build() |
                null
        new MongoClient(new ServerAddress('host:27018'), createMongoX509Credential(),
                MongoClientOptions.builder().serverSelectionTimeout(5).build()) |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(SINGLE)
                        .serverSelectionTimeout(5, MILLISECONDS).build() |
                createMongoX509Credential()
        new MongoClient([new ServerAddress('host:27018')]) |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(MULTIPLE).build() |
                null
        new MongoClient([new ServerAddress('host:27018')], MongoClientOptions.builder().serverSelectionTimeout(5).build()) |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(MULTIPLE)
                        .serverSelectionTimeout(5, MILLISECONDS).build() |
                null
        new MongoClient([new ServerAddress('host:27018')], createMongoX509Credential(),
                MongoClientOptions.builder().serverSelectionTimeout(5).build()) |
                ClusterSettings.builder().hosts([new ServerAddress('host:27018')]).mode(MULTIPLE)
                        .serverSelectionTimeout(5, MILLISECONDS).build() |
                createMongoX509Credential()
    }

    def 'should wrap MongoDBDriverInformation with legacy information'() {
        expect:
        client.delegate.mongoDriverInformation.driverNames == mongoDriverInformation.driverNames
        client.delegate.mongoDriverInformation.driverPlatforms == mongoDriverInformation.driverPlatforms
        client.delegate.mongoDriverInformation.driverVersions == mongoDriverInformation.driverVersions

        cleanup:
        client?.close()

        where:
        client | mongoDriverInformation
        new MongoClient() |
                MongoDriverInformation.builder().driverName('legacy').build()
        new MongoClient('host:27018') |
                MongoDriverInformation.builder().driverName('legacy').build()
        new MongoClient('host', 27018) |
                MongoDriverInformation.builder().driverName('legacy').build()
        new MongoClient('mongodb://host:27018') |
                MongoDriverInformation.builder().driverName('legacy').build()
        new MongoClient(new ConnectionString('mongodb://host:27018')) |
                MongoDriverInformation.builder().driverName('legacy').build()
        new MongoClient(new ConnectionString('mongodb://host:27018'),
                MongoDriverInformation.builder().driverName('test').driverPlatform('osx').driverVersion('1.0').build()) |
                MongoDriverInformation.builder(
                        MongoDriverInformation.builder().driverName('test').driverPlatform('osx').driverVersion('1.0').build())
                        .driverName('legacy').build()
        new MongoClient(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString('mongodb://host:27018')).build()) |
                MongoDriverInformation.builder().driverName('legacy').build()
        new MongoClient(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString('mongodb://host:27018')).build(),
                MongoDriverInformation.builder().driverName('test').driverPlatform('osx').driverVersion('1.0').build()) |
                MongoDriverInformation.builder(
                        MongoDriverInformation.builder().driverName('test').driverPlatform('osx').driverVersion('1.0').build())
                        .driverName('legacy').build()
        new MongoClient(new MongoClientURI('mongodb://host:27018')) |
                MongoDriverInformation.builder().driverName('legacy').build()
        new MongoClient(new MongoClientURI('mongodb://host:27018'),
                MongoDriverInformation.builder().driverName('test').driverPlatform('osx').driverVersion('1.0').build()) |
                MongoDriverInformation.builder(
                        MongoDriverInformation.builder().driverName('test').driverPlatform('osx').driverVersion('1.0').build())
                        .driverName('legacy').build()
        new MongoClient('host:27018', MongoClientOptions.builder().serverSelectionTimeout(5).build()) |
                MongoDriverInformation.builder().driverName('legacy').build()
        new MongoClient(new ServerAddress('host:27018')) |
                MongoDriverInformation.builder().driverName('legacy').build()
        new MongoClient(new ServerAddress('host:27018'), MongoClientOptions.builder().serverSelectionTimeout(5).build()) |
                MongoDriverInformation.builder().driverName('legacy').build()
        new MongoClient(new ServerAddress('host:27018'), createMongoX509Credential(),
                MongoClientOptions.builder().serverSelectionTimeout(5).build()) |
                MongoDriverInformation.builder().driverName('legacy').build()
        new MongoClient(new ServerAddress('host:27018'), createMongoX509Credential(),
                MongoClientOptions.builder().serverSelectionTimeout(5).build(),
                MongoDriverInformation.builder().driverName('test').driverPlatform('osx').driverVersion('1.0').build()) |
                MongoDriverInformation.builder(
                        MongoDriverInformation.builder().driverName('test').driverPlatform('osx').driverVersion('1.0').build())
                        .driverName('legacy').build()
        new MongoClient([new ServerAddress('host:27018')]) |
                MongoDriverInformation.builder().driverName('legacy').build()
        new MongoClient([new ServerAddress('host:27018')], MongoClientOptions.builder().serverSelectionTimeout(5).build()) |
                MongoDriverInformation.builder().driverName('legacy').build()
        new MongoClient([new ServerAddress('host:27018')], MongoClientOptions.builder().serverSelectionTimeout(5).build()) |
                MongoDriverInformation.builder().driverName('legacy').build()
        new MongoClient([new ServerAddress('host:27018')], MongoClientOptions.builder().serverSelectionTimeout(5).build()) |
                MongoDriverInformation.builder().driverName('legacy').build()
        new MongoClient([new ServerAddress('host:27018')], createMongoX509Credential(),
                MongoClientOptions.builder().serverSelectionTimeout(5).build()) |
                MongoDriverInformation.builder().driverName('legacy').build()
        new MongoClient([new ServerAddress('host:27018')], createMongoX509Credential(),
                MongoClientOptions.builder().serverSelectionTimeout(5).build(),
                MongoDriverInformation.builder().driverName('test').driverPlatform('osx').driverVersion('1.0').build()) |
                MongoDriverInformation.builder(
                        MongoDriverInformation.builder().driverName('test').driverPlatform('osx').driverVersion('1.0').build())
                        .driverName('legacy').build()
    }

    def 'should preserve original options'() {
        given:
        def options = MongoClientOptions.builder().cursorFinalizerEnabled(false).build()

        when:
        def client = new MongoClient('localhost', options)

        then:
        client.mongoClientOptions == options

        cleanup:
        client?.close()
    }

    def 'should preserve original options from MongoClientURI'() {
        given:
        def builder = MongoClientOptions.builder().cursorFinalizerEnabled(false)

        when:
        def client = new MongoClient(new MongoClientURI('mongodb://localhost', builder))

        then:
        client.mongoClientOptions == builder.build()

        cleanup:
        client?.close()
    }

    def 'should manage cursor cleaning service if enabled'() {
        when:
        def client = new MongoClient('localhost', MongoClientOptions.builder().cursorFinalizerEnabled(true).build())

        then:
        client.cursorCleaningService != null

        when:
        client.close()

        then:
        client.cursorCleaningService.isShutdown()
    }

    def 'should not create cursor cleaning service if disabled'() {
        when:
        def client = new MongoClient('localhost', MongoClientOptions.builder().cursorFinalizerEnabled(false).build())

        then:
        client.cursorCleaningService == null

        cleanup:
        client?.close()
    }

    def 'should get specified options'() {
        when:
        def options = MongoClientOptions.builder().cursorFinalizerEnabled(false).build()
        def client = new MongoClient('localhost', options)

        then:
        client.mongoClientOptions == options

        cleanup:
        client?.close()
    }

    def 'should get options from specified settings'() {
        when:
        def settings = MongoClientSettings.builder().writeConcern(WriteConcern.MAJORITY).build()
        def client = new MongoClient(settings)

        then:
        client.mongoClientOptions == MongoClientOptions.builder(settings).build()

        cleanup:
        client?.close()
    }

    def 'should validate the ChangeStreamIterable pipeline data correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def client = new MongoClientImpl(Stub(Cluster), null, MongoClientSettings.builder().build(), executor)

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
        expect database, isTheSameAs(new MongoDatabaseImpl('name', client.getCodecRegistry(), secondary(),
                WriteConcern.MAJORITY, true, true, ReadConcern.MAJORITY, STANDARD, null,
                client.getOperationExecutor()))
    }

    def 'should create registry reflecting UuidRepresentation'() {
        given:
        def options = MongoClientOptions.builder()
                .codecRegistry(codecRegistry)
                .uuidRepresentation(C_SHARP_LEGACY)
                .build()

        when:
        def client = new MongoClient('localhost', options)

        then:
        (client.getCodecRegistry().get(UUID) as UuidCodec).getUuidRepresentation() == C_SHARP_LEGACY

        cleanup:
        client?.close()
    }
}
