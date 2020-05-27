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

import com.mongodb.MongoClientException
import com.mongodb.ReadConcern
import com.mongodb.ServerAddress
import com.mongodb.client.model.Collation
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerId
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.bulk.DeleteRequest
import com.mongodb.internal.bulk.IndexRequest
import com.mongodb.internal.bulk.UpdateRequest
import com.mongodb.internal.bulk.WriteRequest
import com.mongodb.internal.connection.AsyncConnection
import com.mongodb.internal.connection.Connection
import com.mongodb.internal.session.SessionContext
import org.bson.BsonDocument
import spock.lang.Specification

import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.WriteConcern.UNACKNOWLEDGED
import static com.mongodb.connection.ServerConnectionState.CONNECTED
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY
import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.internal.operation.OperationHelper.AsyncCallableWithConnection
import static com.mongodb.internal.operation.OperationHelper.AsyncCallableWithConnectionAndSource
import static com.mongodb.internal.operation.OperationHelper.isRetryableRead
import static com.mongodb.internal.operation.OperationHelper.isRetryableWrite
import static com.mongodb.internal.operation.OperationHelper.validateCollation
import static com.mongodb.internal.operation.OperationHelper.validateCollationAndWriteConcern
import static com.mongodb.internal.operation.OperationHelper.validateFindOptions
import static com.mongodb.internal.operation.OperationHelper.validateIndexRequestCollations
import static com.mongodb.internal.operation.OperationHelper.validateReadConcern
import static com.mongodb.internal.operation.OperationHelper.validateWriteRequests

class OperationHelperSpecification extends Specification {

    def 'should accept valid read concern'() {
        given:
        def connection = Stub(Connection) {
            getDescription() >> connectionDescription
        }

        when:
        validateReadConcern(connection, readConcern)

        then:
        notThrown(IllegalArgumentException)

        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateReadConcern(asyncConnection, readConcern, asyncCallableWithConnection)

        then:
        notThrown(IllegalArgumentException)

        when:
        def asyncConnectionSource = Stub(AsyncConnectionSource)
        validateReadConcern(asyncConnectionSource, asyncConnection, readConcern, asyncCallableWithConnectionAndSource)

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

        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateReadConcern(asyncConnection, readConcern, asyncCallableWithConnection)

        then:
        thrown(IllegalArgumentException)

        when:
        def asyncConnectionSource = Stub(AsyncConnectionSource)
        validateReadConcern(asyncConnectionSource, asyncConnection, readConcern, asyncCallableWithConnectionAndSource)

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

        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateCollation(asyncConnection, collation, asyncCallableWithConnection)

        then:
        notThrown(IllegalArgumentException)

        when:
        def asyncConnactionSource = Stub(AsyncConnectionSource)
        validateCollation(asyncConnactionSource, asyncConnection, collation, asyncCallableWithConnectionAndSource)

        then:
        notThrown(IllegalArgumentException)

        when:
        validateCollationAndWriteConcern(connectionDescription, collation, ACKNOWLEDGED)

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

        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateCollation(asyncConnection, collation, asyncCallableWithConnection)

        then:
        thrown(IllegalArgumentException)

        when:
        validateCollationAndWriteConcern(connectionDescription, collation, UNACKNOWLEDGED)

        then:
        thrown(IllegalArgumentException)

        when:

        validateCollationAndWriteConcern(threeSixConnectionDescription, collation, UNACKNOWLEDGED)

        then:
        thrown(MongoClientException)

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

        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateFindOptions(asyncConnection, readConcern, collation, allowDiskUse, asyncCallableWithConnection)

        then:
        notThrown(IllegalArgumentException)

        when:
        def asyncConnactionSource = Stub(AsyncConnectionSource)
        validateFindOptions(asyncConnactionSource, asyncConnection, readConcern, collation, allowDiskUse,
                asyncCallableWithConnectionAndSource)

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

        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateFindOptions(asyncConnection, readConcern, collation, allowDiskUse, asyncCallableWithConnection)

        then:
        thrown(IllegalArgumentException)

        when:
        def asyncConnactionSource = Stub(AsyncConnectionSource)
        validateFindOptions(asyncConnactionSource, asyncConnection, readConcern, collation, allowDiskUse,
                asyncCallableWithConnectionAndSource)

        then:
        thrown(IllegalArgumentException)

        where:
        connectionDescription          | readConcern           | collation    | allowDiskUse
        threeConnectionDescription     | ReadConcern.MAJORITY  | null         | null
        threeConnectionDescription     | ReadConcern.DEFAULT   | enCollation  | null
        threeConnectionDescription     | ReadConcern.DEFAULT   | null         | true
    }

    def 'should accept valid writeRequests'() {
        when:
        validateWriteRequests(connectionDescription, bypassDocumentValidation,  writeRequests, writeConcern)

        then:
        notThrown(IllegalArgumentException)

        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateWriteRequests(asyncConnection, bypassDocumentValidation, writeRequests, writeConcern, asyncCallableWithConnection)

        then:
        notThrown(IllegalArgumentException)

        where:
        connectionDescription          | bypassDocumentValidation | writeConcern   | writeRequests
        threeConnectionDescription     | null                     | ACKNOWLEDGED   | [new DeleteRequest(BsonDocument.parse('{a: "a"}}'))]
        threeTwoConnectionDescription  | null                     | UNACKNOWLEDGED | [new DeleteRequest(BsonDocument.parse('{a: "a"}}'))]
        threeFourConnectionDescription | null                     | ACKNOWLEDGED   | [new DeleteRequest(BsonDocument.parse('{a: "a"}}'))
                                                                                              .collation(enCollation)]
        threeFourConnectionDescription | true                     | ACKNOWLEDGED   | [new UpdateRequest(BsonDocument.parse('{a: "a"}}'),
                                                                                      BsonDocument.parse('{$set: {a: "A"}}'),
                                                                                      WriteRequest.Type.REPLACE).collation(enCollation)]
    }

    def 'should throw on invalid writeRequests'() {
        given:
        def asyncThreeTwoConnection = Stub(AsyncConnection) {
            getDescription() >> threeTwoConnectionDescription
        }
        def writeRequests = [new DeleteRequest(BsonDocument.parse('{a: "a"}}'))]

        when:
        validateWriteRequests(asyncThreeTwoConnection, false, writeRequests, UNACKNOWLEDGED, asyncCallableWithConnection)

        then:
        thrown(MongoClientException)

        when:
        def writeRequestsWithCollation = [new DeleteRequest(BsonDocument.parse('{a: "a"}}'))
                                            .collation(enCollation)]
        validateWriteRequests(asyncThreeTwoConnection, false, writeRequestsWithCollation, ACKNOWLEDGED, asyncCallableWithConnection)

        then:
        thrown(IllegalArgumentException)

        when:
        def asyncThreeFourConnection = Stub(AsyncConnection) {
            getDescription() >> threeFourConnectionDescription
        }
        validateWriteRequests(asyncThreeFourConnection, null, writeRequestsWithCollation, UNACKNOWLEDGED, asyncCallableWithConnection)

        then:
        thrown(MongoClientException)
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

        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateIndexRequestCollations(asyncConnection, indexRequests, asyncCallableWithConnection)

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

        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateIndexRequestCollations(asyncConnection, indexRequests, asyncCallableWithConnection)

        then:
        thrown(IllegalArgumentException)

        where:
        connectionDescription       | indexRequests
        threeConnectionDescription  | [new IndexRequest(BsonDocument.parse('{a: 1}}')).collation(enCollation)]
    }



    def 'should check if a valid retryable write'() {
        given:
        def activeTransactionSessionContext = Stub(SessionContext) {
            hasSession() >> true
            hasActiveTransaction() >> true
        }
        def noTransactionSessionContext = Stub(SessionContext) {
            hasSession() >> true
            hasActiveTransaction() >> false
        }
        def noOpSessionContext = Stub(SessionContext) {
            hasSession() >> false
            hasActiveTransaction() >> false
        }

        expect:
        isRetryableWrite(retryWrites, writeConcern, serverDescription, connectionDescription, noTransactionSessionContext) == expected
        !isRetryableWrite(retryWrites, writeConcern, serverDescription, connectionDescription, activeTransactionSessionContext)
        !isRetryableWrite(retryWrites, writeConcern, serverDescription, connectionDescription, noOpSessionContext)

        where:
        retryWrites | writeConcern   | serverDescription             | connectionDescription                 | expected
        false       | ACKNOWLEDGED   | retryableServerDescription    | threeSixConnectionDescription         | false
        true        | UNACKNOWLEDGED | retryableServerDescription    | threeSixConnectionDescription         | false
        true        | ACKNOWLEDGED   | nonRetryableServerDescription | threeSixConnectionDescription         | false
        true        | ACKNOWLEDGED   | retryableServerDescription    | threeFourConnectionDescription        | false
        true        | ACKNOWLEDGED   | retryableServerDescription    | threeSixConnectionDescription         | false
        true        | ACKNOWLEDGED   | retryableServerDescription    | threeSixPrimaryConnectionDescription  | true
    }

    def 'should check if a valid retryable read'() {
        given:
        def activeTransactionSessionContext = Stub(SessionContext) {
            hasSession() >> true
            hasActiveTransaction() >> true
        }
        def noTransactionSessionContext = Stub(SessionContext) {
            hasSession() >> true
            hasActiveTransaction() >> false
        }
        def noOpSessionContext = Stub(SessionContext) {
            hasSession() >> false
            hasActiveTransaction() >> false
        }

        expect:
        isRetryableRead(retryReads, serverDescription, connectionDescription, noTransactionSessionContext) == expected
        !isRetryableRead(retryReads, serverDescription, connectionDescription, activeTransactionSessionContext)
        !isRetryableRead(retryReads, serverDescription, connectionDescription, noOpSessionContext)

        where:
        retryReads  | serverDescription             | connectionDescription                 | expected
        false       | retryableServerDescription    | threeSixConnectionDescription         | false
        true        | retryableServerDescription    | threeSixConnectionDescription         | true
        true        | nonRetryableServerDescription | threeSixConnectionDescription         | false
        true        | retryableServerDescription    | threeFourConnectionDescription        | false
        true        | retryableServerDescription    | threeSixPrimaryConnectionDescription  | true
    }


    static ConnectionId connectionId = new ConnectionId(new ServerId(new ClusterId(), new ServerAddress()))
    static ConnectionDescription threeSixConnectionDescription = new ConnectionDescription(connectionId, 6,
            STANDALONE, 1000, 100000, 100000, [])
    static ConnectionDescription threeSixPrimaryConnectionDescription = new ConnectionDescription(connectionId, 6,
            REPLICA_SET_PRIMARY, 1000, 100000, 100000, [])
    static ConnectionDescription threeFourConnectionDescription = new ConnectionDescription(connectionId, 5,
            STANDALONE, 1000, 100000, 100000, [])
    static ConnectionDescription threeTwoConnectionDescription = new ConnectionDescription(connectionId, 4,
            STANDALONE, 1000, 100000, 100000, [])
    static ConnectionDescription threeConnectionDescription = new ConnectionDescription(connectionId, 3,
            STANDALONE, 1000, 100000, 100000, [])

    static ServerDescription retryableServerDescription = ServerDescription.builder().address(new ServerAddress()).state(CONNECTED)
            .logicalSessionTimeoutMinutes(1).build()
    static ServerDescription nonRetryableServerDescription = ServerDescription.builder().address(new ServerAddress())
            .state(CONNECTED).build()

    static Collation enCollation = Collation.builder().locale('en').build()

    static asyncCallableWithConnection = new AsyncCallableWithConnection() {
        @Override
        void call(final AsyncConnection conn, final Throwable t) {
            if (t != null) {
                throw t
            }
        }
    }

    static asyncCallableWithConnectionAndSource = new AsyncCallableWithConnectionAndSource() {
        @Override
        void call(final AsyncConnectionSource source, final AsyncConnection conn, final Throwable t) {
            if (t != null) {
                throw t
            }
        }
    }

}
