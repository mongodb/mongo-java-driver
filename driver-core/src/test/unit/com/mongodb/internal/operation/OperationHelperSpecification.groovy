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

import com.mongodb.ServerAddress
import com.mongodb.client.model.Collation
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerId
import com.mongodb.internal.bulk.DeleteRequest
import com.mongodb.internal.bulk.UpdateRequest
import com.mongodb.internal.bulk.WriteRequest
import com.mongodb.internal.connection.AsyncConnection
import com.mongodb.internal.session.SessionContext
import org.bson.BsonDocument
import spock.lang.Specification

import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.WriteConcern.UNACKNOWLEDGED
import static com.mongodb.connection.ServerConnectionState.CONNECTED
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY
import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.internal.operation.OperationHelper.canRetryRead
import static com.mongodb.internal.operation.OperationHelper.isRetryableWrite
import static com.mongodb.internal.operation.OperationHelper.validateWriteRequests

class OperationHelperSpecification extends Specification {

    def 'should accept valid writeRequests'() {
        when:
        validateWriteRequests(connectionDescription, bypassDocumentValidation,  writeRequests, writeConcern)

        then:
        notThrown(IllegalArgumentException)

        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateWriteRequests(asyncConnection.getDescription(), bypassDocumentValidation, writeRequests, writeConcern)

        then:
        notThrown(IllegalArgumentException)

        where:
        connectionDescription          | bypassDocumentValidation | writeConcern   | writeRequests
        threeSixConnectionDescription  | null                     | ACKNOWLEDGED   | [new DeleteRequest(BsonDocument.parse('{a: "a"}}'))]
        threeSixConnectionDescription  | null                     | UNACKNOWLEDGED | [new DeleteRequest(BsonDocument.parse('{a: "a"}}'))]
        threeSixConnectionDescription  | null                     | ACKNOWLEDGED   | [new DeleteRequest(BsonDocument.parse('{a: "a"}}'))
                                                                                              .collation(enCollation)]
        threeSixConnectionDescription  | true                     | ACKNOWLEDGED   | [new UpdateRequest(BsonDocument.parse('{a: "a"}}'),
                                                                                      BsonDocument.parse('{$set: {a: "A"}}'),
                                                                                      WriteRequest.Type.REPLACE).collation(enCollation)]
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
        canRetryRead(serverDescription, noTransactionSessionContext) == expected
        !canRetryRead(serverDescription, activeTransactionSessionContext)
        !canRetryRead(serverDescription, noOpSessionContext)

        where:
        serverDescription             | expected
        retryableServerDescription    | true
        nonRetryableServerDescription | false
    }


    static ConnectionId connectionId = new ConnectionId(new ServerId(new ClusterId(), new ServerAddress()))
    static ConnectionDescription threeSixConnectionDescription = new ConnectionDescription(connectionId, 6,
            STANDALONE, 1000, 100000, 100000, [])
    static ConnectionDescription threeSixPrimaryConnectionDescription = new ConnectionDescription(connectionId, 6,
            REPLICA_SET_PRIMARY, 1000, 100000, 100000, [])
    static ConnectionDescription threeFourConnectionDescription = new ConnectionDescription(connectionId, 5,
            STANDALONE, 1000, 100000, 100000, [])

    static ServerDescription retryableServerDescription = ServerDescription.builder().address(new ServerAddress()).state(CONNECTED)
            .logicalSessionTimeoutMinutes(1).build()
    static ServerDescription nonRetryableServerDescription = ServerDescription.builder().address(new ServerAddress())
            .state(CONNECTED).build()

    static Collation enCollation = Collation.builder().locale('en').build()
}
