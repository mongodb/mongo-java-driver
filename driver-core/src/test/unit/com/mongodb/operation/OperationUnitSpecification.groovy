/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.operation

import com.mongodb.MongoException
import com.mongodb.ReadPreference
import com.mongodb.async.FutureResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.binding.AsyncWriteBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.binding.WriteBinding
import com.mongodb.client.model.Collation
import com.mongodb.client.model.CollationAlternate
import com.mongodb.client.model.CollationCaseFirst
import com.mongodb.client.model.CollationMaxVariable
import com.mongodb.client.model.CollationStrength
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerVersion
import org.bson.BsonDocument
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class OperationUnitSpecification extends Specification {

    void testOperation(operation, List<Integer> serverVersion, BsonDocument expectedCommand, boolean async, BsonDocument result) {
        def test = async ? this.&testAsyncOperation : this.&testSyncOperation
        test(operation, serverVersion, result, true, expectedCommand)
    }

    void testOperationSlaveOk(operation, List<Integer> serverVersion, ReadPreference readPreference, boolean async, result = null) {
        def test = async ? this.&testAsyncOperation : this.&testSyncOperation
        test(operation, serverVersion, result, false, null, true, readPreference)
    }

    void testOperationThrows(operation, List<Integer> serverVersion, boolean async) {
        def test = async ? this.&testAsyncOperation : this.&testSyncOperation
        test(operation, serverVersion, null, false)
    }

    def testSyncOperation(operation, List<Integer> serverVersion, result, Boolean checkCommand=true,
                          BsonDocument expectedCommand=null,
                          Boolean checkSlaveOk=false, ReadPreference readPreference=ReadPreference.primary()) {
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getServerVersion() >> new ServerVersion(serverVersion)
            }
        }

        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> connection
        }
        def readBinding = Stub(ReadBinding) {
            getReadConnectionSource() >> connectionSource
            getReadPreference() >> readPreference
        }
        def writeBinding = Stub(WriteBinding) {
            getWriteConnectionSource() >> connectionSource
        }

        if (checkCommand) {
            1 * connection.command(_, expectedCommand, _, _, _, _) >> { result }
        } else if (checkSlaveOk) {
            1 * connection.command(_, _, _, readPreference, _, _) >> { result }
        }

        0 * connection.command(_, _, _, _, _, _) >> {
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
                           Boolean checkSlaveOk=false, ReadPreference readPreference=ReadPreference.primary()) {
        def connection = Mock(AsyncConnection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getServerVersion() >> new ServerVersion(serverVersion)
            }
        }

        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        def readBinding = Stub(AsyncReadBinding) {
            getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
            getReadPreference() >> readPreference
        }
        def writeBinding = Stub(AsyncWriteBinding) {
            getWriteConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        }
        def callback = new FutureResultCallback()

        if (checkCommand) {
            1 * connection.commandAsync(_, expectedCommand, _, _, _, _, _) >> { it[6].onResult(result, null) }
        } else if (checkSlaveOk) {
            1 * connection.commandAsync(_, _, _, readPreference, _, _, _) >> { it[6].onResult(result, null) }
        }

        0 * connection.commandAsync(_, _, _, _, _, _, _) >> {
            // Unexpected Command
            it[6].onResult(result, null)
        }

        1 * connection.release()

        if (operation instanceof AsyncReadOperation) {
            operation.executeAsync(readBinding, callback)
        } else if (operation instanceof AsyncWriteOperation) {
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
