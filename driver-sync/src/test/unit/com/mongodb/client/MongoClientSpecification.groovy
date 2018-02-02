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

package com.mongodb.client

import com.mongodb.MongoClientSettings
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.client.internal.ListDatabasesIterableImpl
import com.mongodb.client.internal.MongoDatabaseImpl
import com.mongodb.client.internal.TestOperationExecutor
import com.mongodb.connection.Cluster
import com.mongodb.session.ClientSession
import org.bson.BsonDocument
import org.bson.Document
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.client.internal.TestHelper.execute
import static spock.util.matcher.HamcrestSupport.expect

class MongoClientSpecification extends Specification {

    def 'should pass the correct settings to getDatabase'() {
        given:
        def settings = MongoClientSettings.builder()
                .readPreference(secondary())
                .writeConcern(WriteConcern.MAJORITY)
                .readConcern(ReadConcern.MAJORITY)
                .retryWrites(true)
                .build()
        def client = new MongoClientImpl(Stub(Cluster), settings, new TestOperationExecutor([]))

        when:
        def database = client.getDatabase('name')

        then:
        expect database, isTheSameAs(expectedDatabase)

        where:
        expectedDatabase << new MongoDatabaseImpl('name', getDefaultCodecRegistry(), secondary(),
                WriteConcern.MAJORITY, true, ReadConcern.MAJORITY, new TestOperationExecutor([]))
    }

    def 'should use ListDatabasesIterableImpl correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def client = new MongoClientImpl(Stub(Cluster), MongoClientSettings.builder().build(), executor)
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
        def listDatabaseNamesIterable = execute(listDatabasesNamesMethod, session) as MongoIterable<String>

        then:
        // listDatabaseNamesIterable is an instance of a MappingIterable, so have to get the mapped iterable inside it
        expect listDatabaseNamesIterable.getMapped(), isTheSameAs(new ListDatabasesIterableImpl<BsonDocument>(session, BsonDocument,
                getDefaultCodecRegistry(), primary(), executor).nameOnly(true))

        cleanup:
        client?.close()

        where:
        session << [null, Stub(ClientSession)]
    }
}
