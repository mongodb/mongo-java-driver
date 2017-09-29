/*
 * Copyright 2015 MongoDB, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb

import com.mongodb.client.model.geojson.MultiPolygon
import org.bson.BsonDocument
import org.bson.Document
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.MongoClient.getDefaultCodecRegistry
import static com.mongodb.ReadPreference.primary
import static com.mongodb.TestHelper.execute
import static spock.util.matcher.HamcrestSupport.expect

class MongoClientSpecification extends Specification {

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
                getDefaultCodecRegistry(), primary(), executor))

        cleanup:
        client?.close()

        where:
        session << [null, Stub(ClientSession)]
    }
}