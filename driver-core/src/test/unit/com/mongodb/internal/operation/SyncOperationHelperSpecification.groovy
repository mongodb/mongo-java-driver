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

package com.mongodb.internal.operation


import com.mongodb.ReadConcern
import com.mongodb.ServerAddress
import com.mongodb.client.model.Collation
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerId
import com.mongodb.internal.bulk.IndexRequest
import com.mongodb.internal.connection.Connection
import org.bson.BsonDocument
import spock.lang.Specification

import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.internal.operation.SyncOperationHelper.validateCollation
import static com.mongodb.internal.operation.SyncOperationHelper.validateFindOptions
import static com.mongodb.internal.operation.SyncOperationHelper.validateIndexRequestCollations
import static com.mongodb.internal.operation.SyncOperationHelper.validateReadConcern

class SyncOperationHelperSpecification extends Specification {

    def 'should accept valid read concern'() {
        given:
        def connection = Stub(Connection) {
            getDescription() >> connectionDescription
        }

        when:
        validateReadConcern(connection, readConcern)

        then:
        notThrown(IllegalArgumentException)

        where:
        connectionDescription           | readConcern
        threeSixConnectionDescription   | ReadConcern.LOCAL
        threeFourConnectionDescription  | ReadConcern.MAJORITY
        threeTwoConnectionDescription   | ReadConcern.MAJORITY
        threeConnectionDescription      | ReadConcern.DEFAULT
    }

    def 'should throw on invalid read concern'() {
        given:
        def connection = Stub(Connection) {
            getDescription() >> connectionDescription
        }

        when:
        validateReadConcern(connection, readConcern)

        then:
        thrown(IllegalArgumentException)

        where:
        connectionDescription       | readConcern
        threeConnectionDescription  | ReadConcern.LOCAL
        threeConnectionDescription  | ReadConcern.MAJORITY
    }

    def 'should accept valid collation'() {
        given:
        def connection = Stub(Connection) {
            getDescription() >> connectionDescription
        }

        when:
        validateCollation(connection, collation)

        then:
        notThrown(IllegalArgumentException)

        where:
        connectionDescription          | collation
        threeFourConnectionDescription | enCollation
        threeConnectionDescription     | null
    }

    def 'should throw on invalid collation'() {
        given:
        def connection = Stub(Connection) {
            getDescription() >> connectionDescription
        }

        when:
        validateCollation(connection, collation)

        then:
        thrown(IllegalArgumentException)

        where:
        connectionDescription           | collation
        threeTwoConnectionDescription   | enCollation
    }

    def 'should accept valid find options'() {
        given:
        def connection = Stub(Connection) {
            getDescription() >> connectionDescription
        }

        when:
        validateFindOptions(connection, readConcern, collation, allowDiskUse)

        then:
        notThrown(IllegalArgumentException)

        where:
        connectionDescription          | readConcern           | collation    | allowDiskUse
        threeFourConnectionDescription | ReadConcern.DEFAULT   | enCollation  | true
        threeConnectionDescription     | ReadConcern.DEFAULT   | null         | null
    }

    def 'should throw on invalid find options'() {
        given:
        def connection = Stub(Connection) {
            getDescription() >> connectionDescription
        }

        when:
        validateFindOptions(connection, readConcern, collation, allowDiskUse)

        then:
        thrown(IllegalArgumentException)

        where:
        connectionDescription          | readConcern           | collation    | allowDiskUse
        threeConnectionDescription     | ReadConcern.MAJORITY  | null         | null
        threeConnectionDescription     | ReadConcern.DEFAULT   | enCollation  | null
        threeConnectionDescription     | ReadConcern.DEFAULT   | null         | true
    }

    def 'should accept valid indexRequests'() {
        given:
        def connection = Stub(Connection) {
            getDescription() >> connectionDescription
        }

        when:
        validateIndexRequestCollations(connection, indexRequests)

        then:
        notThrown(IllegalArgumentException)

        where:
        connectionDescription          | indexRequests
        threeFourConnectionDescription | [new IndexRequest(BsonDocument.parse('{a: 1}}')).collation(enCollation)]
        threeConnectionDescription     | [new IndexRequest(BsonDocument.parse('{a: 1}}'))]
    }

    def 'should throw on invalid indexRequests'() {
        given:
        def connection = Stub(Connection) {
            getDescription() >> connectionDescription
        }

        when:
        validateIndexRequestCollations(connection, indexRequests)

        then:
        thrown(IllegalArgumentException)

        where:
        connectionDescription       | indexRequests
        threeConnectionDescription  | [new IndexRequest(BsonDocument.parse('{a: 1}}')).collation(enCollation)]
    }

    static ConnectionId connectionId = new ConnectionId(new ServerId(new ClusterId(), new ServerAddress()))
    static ConnectionDescription threeSixConnectionDescription = new ConnectionDescription(connectionId, 6,
            STANDALONE, 1000, 100000, 100000, [])
    static ConnectionDescription threeFourConnectionDescription = new ConnectionDescription(connectionId, 5,
            STANDALONE, 1000, 100000, 100000, [])
    static ConnectionDescription threeTwoConnectionDescription = new ConnectionDescription(connectionId, 4,
            STANDALONE, 1000, 100000, 100000, [])
    static ConnectionDescription threeConnectionDescription = new ConnectionDescription(connectionId, 3,
            STANDALONE, 1000, 100000, 100000, [])

    static Collation enCollation = Collation.builder().locale('en').build()

}
