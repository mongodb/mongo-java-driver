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
import com.mongodb.connection.ServerId
import com.mongodb.internal.ClientSideOperationTimeout
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.bulk.DeleteRequest
import com.mongodb.internal.bulk.IndexRequest
import com.mongodb.internal.bulk.UpdateRequest
import com.mongodb.internal.bulk.WriteRequest
import com.mongodb.internal.connection.AsyncConnection
import org.bson.BsonDocument
import spock.lang.Specification

import static com.mongodb.ClusterFixture.CSOT_NO_TIMEOUT
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.WriteConcern.UNACKNOWLEDGED
import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.internal.operation.AsyncOperationHelper.AsyncCallableWithConnection
import static com.mongodb.internal.operation.AsyncOperationHelper.AsyncCallableWithConnectionAndSource
import static com.mongodb.internal.operation.AsyncOperationHelper.validateCollation
import static com.mongodb.internal.operation.AsyncOperationHelper.validateFindOptions
import static com.mongodb.internal.operation.AsyncOperationHelper.validateIndexRequestCollations
import static com.mongodb.internal.operation.AsyncOperationHelper.validateReadConcern
import static com.mongodb.internal.operation.AsyncOperationHelper.validateWriteRequests

class AsyncOperationHelperSpecification extends Specification {

    def 'should accept valid read concern'() {
        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateReadConcern(CSOT_NO_TIMEOUT, asyncConnection, readConcern, asyncCallableWithConnection)

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
        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateReadConcern(CSOT_NO_TIMEOUT, asyncConnection, readConcern, asyncCallableWithConnection)

        then:
        thrown(IllegalArgumentException)

        where:
        connectionDescription       | readConcern
        threeConnectionDescription  | ReadConcern.LOCAL
        threeConnectionDescription  | ReadConcern.MAJORITY
    }

    def 'should accept valid collation'() {
        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateCollation(CSOT_NO_TIMEOUT, asyncConnection, collation, asyncCallableWithConnection)

        then:
        notThrown(IllegalArgumentException)

        where:
        connectionDescription          | collation
        threeFourConnectionDescription | enCollation
        threeConnectionDescription     | null
    }

    def 'should throw on invalid collation'() {
        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateCollation(CSOT_NO_TIMEOUT, asyncConnection, collation, asyncCallableWithConnection)

        then:
        thrown(IllegalArgumentException)

        where:
        connectionDescription           | collation
        threeTwoConnectionDescription   | enCollation
    }

    def 'should accept valid find options'() {
        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateFindOptions(CSOT_NO_TIMEOUT, asyncConnection, readConcern, collation, allowDiskUse, asyncCallableWithConnection)

        then:
        notThrown(IllegalArgumentException)

        when:
        def asyncConnectionSource = Stub(AsyncConnectionSource)
        validateFindOptions(CSOT_NO_TIMEOUT, asyncConnectionSource, asyncConnection, readConcern, collation, allowDiskUse,
                asyncCallableWithConnectionAndSource)

        then:
        notThrown(IllegalArgumentException)

        where:
        connectionDescription          | readConcern           | collation    | allowDiskUse
        threeFourConnectionDescription | ReadConcern.DEFAULT   | enCollation  | true
        threeConnectionDescription     | ReadConcern.DEFAULT   | null         | null
    }

    def 'should throw on invalid find options'() {
        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateFindOptions(CSOT_NO_TIMEOUT, asyncConnection, readConcern, collation, allowDiskUse, asyncCallableWithConnection)

        then:
        thrown(IllegalArgumentException)

        when:
        def asyncConnectionSource = Stub(AsyncConnectionSource)
        validateFindOptions(CSOT_NO_TIMEOUT, asyncConnectionSource, asyncConnection, readConcern, collation, allowDiskUse,
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
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateWriteRequests(CSOT_NO_TIMEOUT,  asyncConnection, bypassDocumentValidation, writeRequests, writeConcern,
                asyncCallableWithConnection)

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
        validateWriteRequests(CSOT_NO_TIMEOUT, asyncThreeTwoConnection, false, writeRequests, UNACKNOWLEDGED,
                asyncCallableWithConnection)

        then:
        thrown(MongoClientException)

        when:
        def writeRequestsWithCollation = [new DeleteRequest(BsonDocument.parse('{a: "a"}}'))
                                            .collation(enCollation)]
        validateWriteRequests(CSOT_NO_TIMEOUT, asyncThreeTwoConnection, false, writeRequestsWithCollation,
                ACKNOWLEDGED, asyncCallableWithConnection)

        then:
        thrown(IllegalArgumentException)

        when:
        def asyncThreeFourConnection = Stub(AsyncConnection) {
            getDescription() >> threeFourConnectionDescription
        }
        validateWriteRequests(CSOT_NO_TIMEOUT, asyncThreeFourConnection, null, writeRequestsWithCollation, UNACKNOWLEDGED,
                asyncCallableWithConnection)

        then:
        thrown(MongoClientException)
    }

    def 'should accept valid indexRequests'() {
        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateIndexRequestCollations(CSOT_NO_TIMEOUT, asyncConnection, indexRequests, asyncCallableWithConnection)

        then:
        notThrown(IllegalArgumentException)

        where:
        connectionDescription          | indexRequests
        threeFourConnectionDescription | [new IndexRequest(BsonDocument.parse('{a: 1}}')).collation(enCollation)]
        threeConnectionDescription     | [new IndexRequest(BsonDocument.parse('{a: 1}}'))]
    }

    def 'should throw on invalid indexRequests'() {
        when:
        def asyncConnection = Stub(AsyncConnection) {
            getDescription() >> connectionDescription
        }
        validateIndexRequestCollations(CSOT_NO_TIMEOUT, asyncConnection, indexRequests, asyncCallableWithConnection)

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

    static asyncCallableWithConnection = new AsyncCallableWithConnection() {
        @Override
        void call(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnection conn, final Throwable t) {
            if (t != null) {
                throw t
            }
        }
    }

    static asyncCallableWithConnectionAndSource = new AsyncCallableWithConnectionAndSource() {
        @Override
        void call(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnectionSource source,
                  final AsyncConnection conn, final Throwable t) {
            if (t != null) {
                throw t
            }
        }
    }

}
