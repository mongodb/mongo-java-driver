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

package com.mongodb

import com.mongodb.async.FutureResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.binding.AsyncReadWriteBinding
import com.mongodb.binding.AsyncSessionBinding
import com.mongodb.binding.AsyncSingleConnectionBinding
import com.mongodb.binding.AsyncWriteBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.binding.ReadWriteBinding
import com.mongodb.binding.SessionBinding
import com.mongodb.binding.SingleConnectionBinding
import com.mongodb.binding.WriteBinding
import com.mongodb.bulk.InsertRequest
import com.mongodb.client.model.Collation
import com.mongodb.client.model.CollationAlternate
import com.mongodb.client.model.CollationCaseFirst
import com.mongodb.client.model.CollationMaxVariable
import com.mongodb.client.model.CollationStrength
import com.mongodb.client.test.CollectionHelper
import com.mongodb.client.test.Worker
import com.mongodb.client.test.WorkerCodec
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import com.mongodb.connection.ServerVersion
import com.mongodb.connection.SplittablePayload
import com.mongodb.internal.connection.ServerHelper
import com.mongodb.internal.validator.NoOpFieldNameValidator
import com.mongodb.operation.AsyncReadOperation
import com.mongodb.operation.AsyncWriteOperation
import com.mongodb.operation.InsertOperation
import com.mongodb.operation.ReadOperation
import com.mongodb.operation.WriteOperation
import com.mongodb.session.SessionContext
import org.bson.BsonDocument
import org.bson.Document
import org.bson.FieldNameValidator
import org.bson.codecs.DocumentCodec
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.TIMEOUT
import static com.mongodb.ClusterFixture.checkReferenceCountReachesTarget
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.loopCursor
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.operation.OperationUnitSpecification.getMaxWireVersionForServerVersion

class OperationFunctionalSpecification extends Specification {

    def setup() {
        CollectionHelper.drop(getNamespace())
    }

    def cleanup() {
        CollectionHelper.drop(getNamespace())
        checkReferenceCountReachesTarget(getBinding(), 1)
        checkReferenceCountReachesTarget(getAsyncBinding(), 1)
        ServerHelper.checkPool(getPrimary())
    }

    String getDatabaseName() {
        ClusterFixture.getDefaultDatabaseName()
    }

    String getCollectionName() {
        getClass().getName()
    }

    MongoNamespace getNamespace() {
        new MongoNamespace(getDatabaseName(), getCollectionName())
    }

    void acknowledgeWrite(final SingleConnectionBinding binding) {
        new InsertOperation(getNamespace(), true, ACKNOWLEDGED, false, [new InsertRequest(new BsonDocument())]).execute(binding)
        binding.release()
    }

    void acknowledgeWrite(final AsyncSingleConnectionBinding binding) {
        executeAsync(new InsertOperation(getNamespace(), true, ACKNOWLEDGED, false, [new InsertRequest(new BsonDocument())]), binding)
        binding.release()
    }

    CollectionHelper<Document> getCollectionHelper() {
        getCollectionHelper(getNamespace())
    }

    CollectionHelper<Document> getCollectionHelper(MongoNamespace namespace) {
        new CollectionHelper<Document>(new DocumentCodec(), namespace)
    }

    CollectionHelper<Worker> getWorkerCollectionHelper() {
        new CollectionHelper<Worker>(new WorkerCodec(), getNamespace())
    }

    def execute(operation, boolean async) {
        def executor = async ? ClusterFixture.&executeAsync : ClusterFixture.&executeSync
        executor(operation)
    }

    def executeWithSession(operation, boolean async) {
        def executor = async ? ClusterFixture.&executeAsync : ClusterFixture.&executeSync
        def binding = async ? new AsyncSessionBinding(getAsyncBinding()) : new SessionBinding(getBinding())
        executor(operation, binding)
    }

    def execute(operation, ReadWriteBinding binding) {
        ClusterFixture.executeSync(operation, binding)
    }

    def execute(operation, AsyncReadWriteBinding binding) {
        ClusterFixture.executeAsync(operation, binding)
    }

    def executeAndCollectBatchCursorResults(operation, boolean async) {
        def cursor = execute(operation, async)
        def results = []
        if (async) {
            loopCursor([cursor], new Block<Object>(){
                void apply(Object batch) {
                    results.addAll(batch)
                }
            })
        } else {
            while (cursor.hasNext()) {
                results.addAll(cursor.next())
            }
        }
        results
    }

    def next(cursor, boolean async) {
        if (async) {
            def futureResultCallback = new FutureResultCallback<List<BsonDocument>>()
            cursor.next(futureResultCallback)
            futureResultCallback.get(TIMEOUT, TimeUnit.SECONDS)
        } else {
            cursor.next()
        }
    }

    def tryNext(cursor, boolean async) {
        def next
        if (async) {
            def futureResultCallback = new FutureResultCallback<List<BsonDocument>>()
            cursor.tryNext(futureResultCallback)
            next = futureResultCallback.get(TIMEOUT, TimeUnit.SECONDS)
        } else {
            next = cursor.tryNext()
        }
        next
    }

    def consumeAsyncResults(cursor) {
        def batch = next(cursor, true)
        while (batch != null) {
            batch = next(cursor, true)
        }
    }

    void testOperation(Map params) {
        params.async = params.async ?: false
        params.result = params.result ?: null
        params.checkCommand = params.checkCommand ?: true
        params.checkSlaveOk = params.checkSlaveOk ?: false
        params.readPreference = params.readPreference ?: ReadPreference.primary()
        params.retryable = params.retryable ?: false
        params.serverType = params.serverType ?: ServerType.STANDALONE
        testOperation(params.operation, params.serverVersion, params.expectedCommand, params.async, params.result, params.checkCommand,
                params.checkSlaveOk, params.readPreference, params.retryable, params.serverType)
    }

    void testOperationInTransaction(operation, List<Integer> serverVersion, BsonDocument expectedCommand, boolean async, result = null,
                                    boolean checkCommand = true, boolean checkSlaveOk = false,
                                    ReadPreference readPreference = ReadPreference.primary(), boolean retryable = false,
                                    ServerType serverType = ServerType.STANDALONE) {
        testOperation(operation, serverVersion, ReadConcern.DEFAULT, expectedCommand, async, result, checkCommand, checkSlaveOk,
                readPreference, retryable, serverType, true)
    }

    void testOperation(operation, List<Integer> serverVersion, BsonDocument expectedCommand, boolean async, result = null,
                       boolean checkCommand = true, boolean checkSlaveOk = false, ReadPreference readPreference = ReadPreference.primary(),
                       boolean retryable = false, ServerType serverType = ServerType.STANDALONE, Boolean activeTransaction = false) {
        testOperation(operation, serverVersion, ReadConcern.DEFAULT, expectedCommand, async, result, checkCommand, checkSlaveOk,
        readPreference, retryable, serverType, activeTransaction)
    }

    void testOperation(operation, List<Integer> serverVersion, ReadConcern readConcern, BsonDocument expectedCommand, boolean async,
                       result = null, boolean checkCommand = true, boolean checkSlaveOk = false,
                       ReadPreference readPreference = ReadPreference.primary(), boolean retryable = false,
                       ServerType serverType = ServerType.STANDALONE, Boolean activeTransaction = false) {
        def test = async ? this.&testAsyncOperation : this.&testSyncOperation
        test(operation, serverVersion, readConcern, result, checkCommand, expectedCommand, checkSlaveOk, readPreference, retryable,
                serverType, activeTransaction)
    }

    void testOperationRetries(operation, List<Integer> serverVersion, BsonDocument expectedCommand, boolean async, result = null,
                              Boolean activeTransaction = false) {
        testOperation(operation, serverVersion, expectedCommand, async, result, true, false, ReadPreference.primary(), true,
             ServerType.REPLICA_SET_PRIMARY, activeTransaction)
    }

    void testRetryableOperationThrowsOriginalError(operation, List<List<Integer>> serverVersions, List<ServerType> serverTypes,
                                                   Throwable exception, boolean async) {
        def test = async ? this.&testAyncRetryableOperationThrows : this.&testSyncRetryableOperationThrows
        test(operation, serverVersions as Queue, serverTypes as Queue, exception)
    }

    void testOperationSlaveOk(operation, List<Integer> serverVersion, ReadPreference readPreference, boolean async, result = null) {
        def test = async ? this.&testAsyncOperation : this.&testSyncOperation
        test(operation, serverVersion, ReadConcern.DEFAULT, result, false, null, true, readPreference)
    }

    void testOperationThrows(operation, List<Integer> serverVersion, boolean async) {
        testOperationThrows(operation, serverVersion, ReadConcern.DEFAULT, async)
    }

    void testOperationThrows(operation, List<Integer> serverVersion, ReadConcern readConcern, boolean async) {
        def test = async ? this.&testAsyncOperation : this.&testSyncOperation
        test(operation, serverVersion, readConcern, null, false, null, false, ReadPreference.primary(),
                false, ServerType.STANDALONE, false)
    }

    def testSyncOperation(operation, List<Integer> serverVersion, ReadConcern readConcern, result, Boolean checkCommand=true,
                          BsonDocument expectedCommand=null, Boolean checkSlaveOk=false,
                          ReadPreference readPreference=ReadPreference.primary(), Boolean retryable = false,
                          ServerType serverType = ServerType.STANDALONE, Boolean activeTransaction = false) {
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> getMaxWireVersionForServerVersion(serverVersion)
                getServerType() >> serverType
            }
        }

        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> {
                connection
            }
            getServerDescription() >> {
                def builder = ServerDescription.builder().address(Stub(ServerAddress)).state(ServerConnectionState.CONNECTED)
                if (new ServerVersion(serverVersion).compareTo(new ServerVersion(3, 6)) >= 0) {
                    builder.logicalSessionTimeoutMinutes(42)
                }
                builder.build()
            }
        }
        def readBinding = Stub(ReadBinding) {
            getReadConnectionSource() >> connectionSource
            getReadPreference() >> readPreference
            getSessionContext() >> Stub(SessionContext) {
                hasSession() >> true
                hasActiveTransaction() >> activeTransaction
                getReadConcern() >> readConcern
            }
        }
        def writeBinding = Stub(WriteBinding) {
            getWriteConnectionSource() >> connectionSource
            getSessionContext() >> Stub(SessionContext) {
                hasSession() >> true
                hasActiveTransaction() >> activeTransaction
                getReadConcern() >> readConcern
            }
        }

        if (retryable) {
            1 * connection.command(*_) >> { throw new MongoSocketException('Some socket error', Stub(ServerAddress)) }
        }

        if (checkCommand) {
            1 * connection.command(*_) >> {
                assert it[1] == expectedCommand
                if (it.size() == 9) {
                    SplittablePayload payload = it[7]
                    payload.setPosition(payload.getPayload().size())
                }
                result
            }
        } else if (checkSlaveOk) {
            1 * connection.command(*_) >> {
                it[4] == readPreference
                result
            }
        }

        0 * connection.command(_, _, _, _, _, _) >> {
            // Unexpected Command
            result
        }

        if (retryable) {
            2 * connection.release()
        } else {
            1 * connection.release()
        }
        if (operation instanceof ReadOperation) {
            operation.execute(readBinding)
        } else if (operation instanceof WriteOperation) {
            operation.execute(writeBinding)
        }
    }

    def testAsyncOperation(operation = operation, List<Integer> serverVersion = serverVersion, ReadConcern readConcern, result = null,
                           Boolean checkCommand = true, BsonDocument expectedCommand = null, Boolean checkSlaveOk = false,
                           ReadPreference readPreference = ReadPreference.primary(), Boolean retryable = false,
                           ServerType serverType = ServerType.STANDALONE, Boolean activeTransaction = false) {
        def connection = Mock(AsyncConnection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> getMaxWireVersionForServerVersion(serverVersion)
                getServerType() >> serverType
            }
        }

        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
            getServerDescription() >> {
                def builder = ServerDescription.builder().address(Stub(ServerAddress)).state(ServerConnectionState.CONNECTED)
                if (new ServerVersion(serverVersion).compareTo(new ServerVersion(3, 6)) >= 0) {
                    builder.logicalSessionTimeoutMinutes(42)
                }
                builder.build()
            }
        }
        def readBinding = Stub(AsyncReadBinding) {
            getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
            getReadPreference() >> readPreference
            getSessionContext() >> Stub(SessionContext) {
                hasSession() >> true
                hasActiveTransaction() >> activeTransaction
                getReadConcern() >> readConcern
            }
        }
        def writeBinding = Stub(AsyncWriteBinding) {
            getWriteConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
            getSessionContext() >> Stub(SessionContext) {
                hasSession() >> true
                hasActiveTransaction() >> activeTransaction
                getReadConcern() >> readConcern
            }
        }
        def callback = new FutureResultCallback()

        if (retryable) {
            1 * connection.commandAsync(*_) >> {
                it.last().onResult(null, new MongoSocketException('Some socket error', Stub(ServerAddress)))
            }
        }

        if (checkCommand) {
            1 * connection.commandAsync(*_) >> {
                assert it[1] == expectedCommand
                if (it.size() == 10) {
                    SplittablePayload payload = it[7]
                    payload.setPosition(payload.getPayload().size())
                }
                it.last().onResult(result, null)
            }
        } else if (checkSlaveOk) {
            1 * connection.commandAsync(*_) >> {
                it[4] == readPreference
                it.last().onResult(result, null)
            }
        }

        0 * connection.commandAsync(_, _, _, _, _, _, _) >> {
            // Unexpected Command
            it[5].onResult(result, null)
        }

        if (retryable) {
            2 * connection.release()
        } else {
            1 * connection.release()
        }

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

    def testSyncRetryableOperationThrows(operation, Queue<List<Integer>> serverVersions, Queue<ServerType> serverTypes,
                                         Throwable exception) {
        def serverVersionSize = serverVersions.size()
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> {
                    getMaxWireVersionForServerVersion(serverVersions.poll())
                }
                getServerType() >> {
                    serverTypes.poll()
                }
            }
        }

        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> {
                if (serverVersions.isEmpty()){
                    throw new MongoSocketOpenException('No Server', new ServerAddress(), new Exception('no server'))
                } else {
                    connection
                }
            }
        }
        def writeBinding = Stub(WriteBinding) {
            getWriteConnectionSource() >> connectionSource
            getSessionContext() >> Stub(SessionContext) {
                hasSession() >> true
                hasActiveTransaction() >> false
                getReadConcern() >> ReadConcern.DEFAULT
            }
        }

        1 * connection.command(*_) >> { throw exception }

        if (serverVersionSize == 1) {
            1 * connection.release()
        } else {
            2 * connection.release()
        }
        operation.execute(writeBinding)
    }

    def testAyncRetryableOperationThrows(operation, Queue<List<Integer>> serverVersions, Queue<ServerType> serverTypes,
                                         Throwable exception) {
        def serverVersionSize = serverVersions.size()
        def connection = Mock(AsyncConnection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> {
                    getMaxWireVersionForServerVersion(serverVersions.poll())
                }
                getServerType() >> {
                    serverTypes.poll()
                }
            }
        }

        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> {
                if (serverVersions.isEmpty()) {
                    it[0].onResult(null,
                            new MongoSocketOpenException('No Server', new ServerAddress(), new Exception('no server')))
                } else {
                    it[0].onResult(connection, null)
                }
            }
        }

        def writeBinding = Stub(AsyncWriteBinding) {
            getWriteConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
            getSessionContext() >> Stub(SessionContext) {
                hasSession() >> true
                hasActiveTransaction() >> false
                getReadConcern() >> ReadConcern.DEFAULT
            }
        }
        def callback = new FutureResultCallback()

        1 * connection.commandAsync(*_) >> { it.last().onResult(null, exception) }
        if (serverVersionSize == 1) {
            1 * connection.release()
        } else {
            2 * connection.release()
        }

        operation.executeAsync(writeBinding, callback)
        callback.get(1000, TimeUnit.MILLISECONDS)
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

    static final FieldNameValidator NO_OP_FIELD_NAME_VALIDATOR = new NoOpFieldNameValidator()

    static boolean serverVersionIsGreaterThan(List<Integer> actualVersion, List<Integer> minVersion) {
        new ServerVersion(actualVersion).compareTo(new ServerVersion(minVersion)) >= 0
    }
}
