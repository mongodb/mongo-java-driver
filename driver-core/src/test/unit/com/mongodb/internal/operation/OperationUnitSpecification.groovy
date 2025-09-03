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

import com.mongodb.MongoException
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.async.FutureResultCallback
import com.mongodb.client.model.Collation
import com.mongodb.client.model.CollationAlternate
import com.mongodb.client.model.CollationCaseFirst
import com.mongodb.client.model.CollationMaxVariable
import com.mongodb.client.model.CollationStrength
import com.mongodb.connection.ConnectionDescription
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.binding.AsyncReadBinding
import com.mongodb.internal.binding.AsyncWriteBinding
import com.mongodb.internal.binding.ConnectionSource
import com.mongodb.internal.binding.ReadBinding
import com.mongodb.internal.binding.WriteBinding
import com.mongodb.internal.connection.AsyncConnection
import com.mongodb.internal.connection.Connection
import com.mongodb.internal.session.SessionContext
import org.bson.BsonDocument
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT

class OperationUnitSpecification extends Specification {

    // Have to add to this map for every server release
    private static final SERVER_TO_WIRE_VERSION_MAP = [
            [2, 6]: 2,
            [3, 0]: 3,
            [3, 2]: 4,
            [3, 4]: 5,
            [3, 6]: 6,
            [4, 0]: 7,
            [4, 1]: 8,
            [4, 2]: 8,
            [4, 4]: 9,
            [5, 0]: 13,
            [5, 1]: 14,
            [5, 2]: 15,
            [5, 3]: 16,
            [6, 0]: 17,
            [6, 1]: 18,
            [6, 2]: 19,
            [6, 3]: 20,
            [7, 0]: 21,
            [9, 0]: 25,
    ]

    static Integer getMaxWireVersionForServerVersion(List<Integer> serverVersion) {
        def maxWireVersion = SERVER_TO_WIRE_VERSION_MAP[serverVersion.subList(0, 2)]

        if (maxWireVersion == null) {
            throw new IllegalArgumentException('Unknown server version ' + serverVersion.subList(0, 2) + '.  Check if it has been added ' +
                    'to SERVER_TO_WIRE_VERSION_MAP')
        }

        maxWireVersion
    }

    void testOperation(operation, List<Integer> serverVersion, BsonDocument expectedCommand, boolean async, BsonDocument result) {
        def test = async ? this.&testAsyncOperation : this.&testSyncOperation
        test(operation, serverVersion, result, true, expectedCommand)
    }

    void testOperationSecondaryOk(operation, List<Integer> serverVersion, ReadPreference readPreference, boolean async, result = null) {
        def test = async ? this.&testAsyncOperation : this.&testSyncOperation
        test(operation, serverVersion, result, false, null, true, readPreference)
    }

    void testOperationThrows(operation, List<Integer> serverVersion, boolean async) {
        def test = async ? this.&testAsyncOperation : this.&testSyncOperation
        test(operation, serverVersion, null, false)
    }

    def testSyncOperation(operation, List<Integer> serverVersion, result, Boolean checkCommand=true,
                          BsonDocument expectedCommand=null,
                          Boolean checkSecondaryOk=false, ReadPreference readPreference=ReadPreference.primary()) {
        def operationContext = OPERATION_CONTEXT
                .withSessionContext(Stub(SessionContext) {
                    hasActiveTransaction() >> false
                    getReadConcern() >> ReadConcern.DEFAULT
                })

        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> getMaxWireVersionForServerVersion(serverVersion)
            }
        }

        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> connection
            getReadPreference() >> readPreference
            getOperationContext() >> operationContext
        }
        def readBinding = Stub(ReadBinding) {
            getReadConnectionSource() >> connectionSource
            getReadPreference() >> readPreference
            getOperationContext() >> operationContext
        }
        def writeBinding = Stub(WriteBinding) {
            getWriteConnectionSource() >> connectionSource
            getOperationContext() >> operationContext
        }

        if (checkCommand) {
            1 * connection.command(*_) >> {
                assert(it[1] == expectedCommand)
                result
            }
        } else if (checkSecondaryOk) {
            1 * connection.command(*_) >> {
                assert(it[3] == readPreference)
                result
            }
        }

        0 * connection.command(*_) >> {
            // Unexpected Command
            result
        }

        1 * connection.release()

        if (operation instanceof ReadOperation) {
            operation.execute(readBinding)
        } else if (operation instanceof WriteOperation) {
            operation.execute(writeBinding)
        }
    }

    def testAsyncOperation(operation, List<Integer> serverVersion, result = null,
                           Boolean checkCommand=true, BsonDocument expectedCommand=null,
                           Boolean checkSecondaryOk=false, ReadPreference readPreference=ReadPreference.primary()) {

        def operationContext = OPERATION_CONTEXT
                .withSessionContext(Stub(SessionContext) {
                    hasActiveTransaction() >> false
                    getReadConcern() >> ReadConcern.DEFAULT
                })

        def connection = Mock(AsyncConnection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> getMaxWireVersionForServerVersion(serverVersion)
            }
        }

        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
            getReadPreference() >> readPreference
            getOperationContext() >> getOperationContext() >> operationContext
        }
        def readBinding = Stub(AsyncReadBinding) {
            getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
            getReadPreference() >> readPreference
            getOperationContext() >> operationContext
        }
        def writeBinding = Stub(AsyncWriteBinding) {
            getWriteConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
            getOperationContext() >> operationContext
        }
        def callback = new FutureResultCallback()

        if (checkCommand) {
            1 * connection.commandAsync(*_) >> {
                assert(it[1] == expectedCommand)
                it.last().onResult(result, null)
            }
        } else if (checkSecondaryOk) {
            1 * connection.commandAsync(*_) >> {
                assert(it[3] == readPreference)
                it.last().onResult(result, null)
            }
        }

        0 * connection.commandAsync(_, _, _, _, _, _, _, _) >> {
            // Unexpected Command
            it.last().onResult(result, null)
        }

        1 * connection.release()

        if (operation instanceof ReadOperation) {
            operation.executeAsync(readBinding, callback)
        } else if (operation instanceof WriteOperation) {
            operation.executeAsync(writeBinding, callback)
        }
         try {
             callback.get(1000, TimeUnit.MILLISECONDS)
         } catch (MongoException e) {
            throw e.cause
        }
    }

    @Shared
    Collation defaultCollation = Collation.builder()
            .locale('en')
            .caseLevel(true)
            .collationCaseFirst(CollationCaseFirst.OFF)
            .collationStrength(CollationStrength.IDENTICAL)
            .numericOrdering(true)
            .collationAlternate(CollationAlternate.SHIFTED)
            .collationMaxVariable(CollationMaxVariable.SPACE)
            .normalization(true)
            .backwards(true)
            .build()

    @Shared
    Collation caseInsensitiveCollation = Collation.builder()
            .locale('en')
            .collationStrength(CollationStrength.SECONDARY)
            .build()
}
