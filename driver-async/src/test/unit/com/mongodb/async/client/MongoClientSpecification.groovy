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

package com.mongodb.async.client

import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.client.model.geojson.MultiPolygon
import com.mongodb.connection.Cluster
import com.mongodb.session.ClientSession
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.async.client.MongoClients.getDefaultCodecRegistry
import static com.mongodb.async.client.TestHelper.execute
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class MongoClientSpecification extends Specification {

    def 'should use ListDatabasesIterableImpl correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def client = new MongoClientImpl(MongoClientSettings.builder().build(), Stub(Cluster), executor)
        def listDatabasesMethod = client.&listDatabases
        def listDatabasesNamesMethod = client.&listDatabaseNames

        when:
        def listDatabasesIterable = execute(listDatabasesMethod, session)

        then:
        expect listDatabasesIterable, isTheSameAs(new ListDatabasesIterableImpl<Document>(session, Document, getDefaultCodecRegistry(),
                primary(), executor))

        when:
        listDatabasesIterable = execute(listDatabasesMethod, session, BsonDocument)

        then:
        expect listDatabasesIterable, isTheSameAs(new ListDatabasesIterableImpl<BsonDocument>(session, BsonDocument,
                getDefaultCodecRegistry(), primary(), executor))

        when:
        def listDatabaseNamesIterable = execute(listDatabasesNamesMethod, session)

        then:
        // listDatabaseNamesIterable is an instance of a MappingIterable, so have to get the mapped iterable inside it
        expect listDatabaseNamesIterable.getMapped(), isTheSameAs(new ListDatabasesIterableImpl<BsonDocument>(session, BsonDocument,
                getDefaultCodecRegistry(), primary(), executor).nameOnly(true))

        cleanup:
        client?.close()

        where:
        session << [null, Stub(ClientSession)]
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
                                          .retryWrites(true)
                                          .readConcern(ReadConcern.MAJORITY)
                                          .codecRegistry(codecRegistry)
                                          .build()
        def client = new MongoClientImpl(settings, Stub(Cluster), new TestOperationExecutor([]))

        when:
        def database = client.getDatabase('name')

        then:
        expect database, isTheSameAs(expectedDatabase)

        where:
        expectedDatabase << new MongoDatabaseImpl('name', fromProviders([new BsonValueCodecProvider()]), secondary(),
                WriteConcern.MAJORITY, true, ReadConcern.MAJORITY, new TestOperationExecutor([]))
    }


    def 'should cleanly close the external resource closer on close'() {
        given:
        def closed = false
        def client = new MongoClientImpl(MongoClientSettings.builder().build(), Mock(Cluster), {
            closed = true
            throw new IOException()
        })

        when:
        client.close()

        then:
        closed
    }

    def 'default codec registry should contain all supported providers'() {
        given:
        def codecRegistry = getDefaultCodecRegistry()

        expect:
        codecRegistry.get(BsonDocument)
        codecRegistry.get(Document)
        codecRegistry.get(Integer)
        codecRegistry.get(MultiPolygon)
        codecRegistry.get(Iterable)
    }
}
