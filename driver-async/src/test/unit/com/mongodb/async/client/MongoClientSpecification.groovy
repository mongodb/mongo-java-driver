/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.async.client

import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.client.model.geojson.MultiPolygon
import com.mongodb.connection.Cluster
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class MongoClientSpecification extends Specification {

    def 'should use ListCollectionsOperation correctly'() {
        given:
        def settings = MongoClientSettings.builder().build()
        def cluster = Stub(Cluster)
        def executor = new TestOperationExecutor([null, null, null])
        def client = new MongoClientImpl(settings, cluster, executor)
        def codecRegistry = MongoClients.getDefaultCodecRegistry()

        when:
        def listDatabasesIterable = client.listDatabases()

        then:
        expect listDatabasesIterable, isTheSameAs(new ListDatabasesIterableImpl<Document>(Document, codecRegistry, primary(),
                executor))

        when:
        listDatabasesIterable = client.listDatabases(BsonDocument)

        then:
        expect listDatabasesIterable, isTheSameAs(new ListDatabasesIterableImpl<BsonDocument>(BsonDocument, codecRegistry, primary(),
                executor))
    }

    def 'should provide the same settings'() {
        given:
        def settings = MongoClientSettings.builder().build()

        when:
        def clientSettings = new MongoClientImpl(settings, Stub(Cluster), new TestOperationExecutor([])).getSettings()

        then:
        settings == clientSettings
    }

    def 'should pass the correct settings to getDatabase'() {
        given:
        def codecRegistry = fromProviders([new BsonValueCodecProvider()])
        def settings = MongoClientSettings.builder()
                                          .readPreference(secondary())
                                          .writeConcern(WriteConcern.MAJORITY)
                                          .readConcern(ReadConcern.MAJORITY)
                                          .codecRegistry(codecRegistry)
                                          .build()
        def client = new MongoClientImpl(settings, Stub(Cluster), new TestOperationExecutor([]))

        when:
        def database = client.getDatabase('name');

        then:
        expect database, isTheSameAs(expectedDatabase)

        where:
        expectedDatabase << new MongoDatabaseImpl('name', fromProviders([new BsonValueCodecProvider()]), secondary(),
                WriteConcern.MAJORITY, ReadConcern.MAJORITY, new TestOperationExecutor([]))
    }

    def 'default codec registry should contain all supported providers'() {
        given:
        def codecRegistry = MongoClients.getDefaultCodecRegistry()

        expect:
        codecRegistry.get(BsonDocument)
        codecRegistry.get(Document)
        codecRegistry.get(Integer)
        codecRegistry.get(MultiPolygon)
        codecRegistry.get(Iterable)
    }
}
