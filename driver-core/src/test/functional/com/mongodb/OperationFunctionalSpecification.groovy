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
import com.mongodb.client.model.Collation
import com.mongodb.client.model.CollationAlternate
import com.mongodb.client.model.CollationCaseFirst
import com.mongodb.client.model.CollationMaxVariable
import com.mongodb.client.model.CollationStrength
import com.mongodb.client.test.CollectionHelper
import com.mongodb.client.test.Worker
import com.mongodb.client.test.WorkerCodec
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import com.mongodb.connection.ServerVersion
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.binding.AsyncReadBinding
import com.mongodb.internal.binding.AsyncReadWriteBinding
import com.mongodb.internal.binding.AsyncSessionBinding
import com.mongodb.internal.binding.AsyncSingleConnectionBinding
import com.mongodb.internal.binding.AsyncWriteBinding
import com.mongodb.internal.binding.ConnectionSource
import com.mongodb.internal.binding.ReadBinding
import com.mongodb.internal.binding.ReadWriteBinding
import com.mongodb.internal.binding.SessionBinding
import com.mongodb.internal.binding.SingleConnectionBinding
import com.mongodb.internal.binding.WriteBinding
import com.mongodb.internal.bulk.InsertRequest
import com.mongodb.internal.connection.AsyncConnection
import com.mongodb.internal.connection.Connection
import com.mongodb.internal.connection.ServerHelper
import com.mongodb.internal.connection.SplittablePayload
import com.mongodb.internal.operation.AsyncReadOperation
import com.mongodb.internal.operation.AsyncWriteOperation
import com.mongodb.internal.operation.InsertOperation
import com.mongodb.internal.operation.ReadOperation
import com.mongodb.internal.operation.WriteOperation
import com.mongodb.internal.session.SessionContext
import com.mongodb.internal.validator.NoOpFieldNameValidator
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
import static com.mongodb.internal.operation.OperationUnitSpecification.getMaxWireVersionForServerVersion

class OperationFunctionalSpecification extends Specification {

    def setup() {
        ServerHelper.checkPool(getPrimary())
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

    def next(cursor, boolean async, int minimumCount) {
        next(cursor, async, false, minimumCount)
    }

    def next(cursor, boolean async, boolean callHasNextBeforeNext, int minimumCount) {
        List<BsonDocument> retVal = []

        while (retVal.size() < minimumCount) {
            retVal.addAll(doNext(cursor, async, callHasNextBeforeNext))
        }

        retVal
    }

    def next(cursor, boolean async) {
        doNext(cursor, async, false)
    }

    def doNext(cursor, boolean async, boolean callHasNextBeforeNext) {
        if (async) {
            def futureResultCallback = new FutureResultCallback<List<BsonDocument>>()
            cursor.next(futureResultCallback)
            futureResultCallback.get(TIMEOUT, TimeUnit.SECONDS)
        } else {
            if (callHasNextBeforeNext) {
                cursor.hasNext()
            }
            cursor.next()
        }
    }

    def consumeAsyncResults(cursor) {
        def batch = next(cursor, true)
        while (batch != null) {
            batch = next(cursor, true)
        }
    }

    void testOperation(Map params) {
        params.async = params.async != null ? params.async : false
        params.result = params.result != null ? params.result : null
        params.checkCommand = params.checkCommand != null ? params.checkCommand : true
        params.checkSecondaryOk = params.checkSecondaryOk != null ? params.checkSecondaryOk : false
        params.readPreference = params.readPreference != null ? params.readPreference : ReadPreference.primary()
        params.retryable = params.retryable != null ? params.retryable : false
        params.serverType = params.serverType != null ? params.serverType : ServerType.STANDALONE
        testOperation(params.operation, params.serverVersion, params.expectedCommand, params.async, params.result, params.checkCommand,
                params.checkSecondaryOk, params.readPreference, params.retryable, params.serverType)
    }

    void testOperationInTransaction(operation, List<Integer> serverVersion, BsonDocument expectedCommand, boolean async, result = null,
                                    boolean checkCommand = true, boolean checkSecondaryOk = false,
                                    ReadPreference readPreference = ReadPreference.primary(), boolean retryable = false,
                                    ServerType serverType = ServerType.STANDALONE) {
        testOperation(operation, serverVersion, ReadConcern.DEFAULT, expectedCommand, async, result, checkCommand, checkSecondaryOk,
                readPreference, retryable, serverType, true)
    }

    void testOperation(operation, List<Integer> serverVersion, BsonDocument expectedCommand, boolean async, result = null,
                       boolean checkCommand = true, boolean checkSecondaryOk = false,
                       ReadPreference readPreference = ReadPreference.primary(), boolean retryable = false,
                       ServerType serverType = ServerType.STANDALONE, Boolean activeTransaction = false) {
        testOperation(operation, serverVersion, ReadConcern.DEFAULT, expectedCommand, async, result, checkCommand, checkSecondaryOk,
        readPreference, retryable, serverType, activeTransaction)
    }

    void testOperation(operation, List<Integer> serverVersion, ReadConcern readConcern, BsonDocument expectedCommand, boolean async,
                       result = null, boolean checkCommand = true, boolean checkSecondaryOk = false,
                       ReadPreference readPreference = ReadPreference.primary(), boolean retryable = false,
                       ServerType serverType = ServerType.STANDALONE, Boolean activeTransaction = false) {
        def test = async ? this.&testAsyncOperation : this.&testSyncOperation
        test(operation, serverVersion, readConcern, result, checkCommand, expectedCommand, checkSecondaryOk, readPreference, retryable,
                serverType, activeTransaction)
    }

    void testOperationRetries(operation, List<Integer> serverVersion, BsonDocument expectedCommand, boolean async, result = null,
                              Boolean activeTransaction = false) {
        testOperation(operation, serverVersion, expectedCommand, async, result, true, false, ReadPreference.primary(), true,
             ServerType.REPLICA_SET_PRIMARY, activeTransaction)
    }

    void testRetryableOperationThrowsOriginalError(operation, List<List<Integer>> serverVersions, List<ServerType> serverTypes,
                                                   Throwable exception, boolean async, int expectedConnectionReleaseCount = 2) {
        def test = async ? this.&testAyncRetryableOperationThrows : this.&testSyncRetryableOperationThrows
        test(operation, serverVersions as Queue, serverTypes as Queue, exception, expectedConnectionReleaseCount)
    }

    void testOperationSecondaryOk(operation, List<Integer> serverVersion, ReadPreference readPreference, boolean async, result = null) {
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
                          BsonDocument expectedCommand=null, Boolean checkSecondaryOk=false,
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
            getServerApi() >> null
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
            getServerApi() >> null
            getSessionContext() >> Stub(SessionContext) {
                hasSession() >> true
                hasActiveTransaction() >> activeTransaction
                getReadConcern() >> readConcern
            }
        }
        def writeBinding = Stub(WriteBinding) {
            getWriteConnectionSource() >> connectionSource
            getServerApi() >> null
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
                if (it.size() == 11) {
                    SplittablePayload payload = it[9]
                    payload.setPosition(payload.size())
                }
                result
            }
        } else if (checkSecondaryOk) {
            1 * connection.command(*_) >> {
                it[4] == readPreference
                result
            }
        }

        0 * connection.command(*_) >> {
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
                           Boolean checkCommand = true, BsonDocument expectedCommand = null, Boolean checkSecondaryOk = false,
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
            getServerApi() >> null
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
            getServerApi() >> null
            getSessionContext() >> Stub(SessionContext) {
                hasSession() >> true
                hasActiveTransaction() >> activeTransaction
                getReadConcern() >> readConcern
            }
        }
        def writeBinding = Stub(AsyncWriteBinding) {
            getWriteConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
            getServerApi() >> null
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
                if (it.size() == 12) {
                    SplittablePayload payload = it[9]
                    payload.setPosition(payload.size())
                }
                it.last().onResult(result, null)
            }
        } else if (checkSecondaryOk) {
            1 * connection.commandAsync(*_) >> {
                it[4] == readPreference
                it.last().onResult(result, null)
            }
        }

        0 * connection.commandAsync(*_) >> {
            // Unexpected Command
            it.last().onResult(result, null)
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
                                         Throwable exception, int expectedConnectionReleaseCount) {
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
            getServerApi() >> null
        }
        def writeBinding = Stub(WriteBinding) {
            getWriteConnectionSource() >> connectionSource
            getServerApi() >> null
            getSessionContext() >> Stub(SessionContext) {
                hasSession() >> true
                hasActiveTransaction() >> false
                getReadConcern() >> ReadConcern.DEFAULT
            }
        }

        1 * connection.command(*_) >> {
            throw exception
        }

        expectedConnectionReleaseCount * connection.release()
        operation.execute(writeBinding)
    }

    def testAyncRetryableOperationThrows(operation, Queue<List<Integer>> serverVersions, Queue<ServerType> serverTypes,
                                         Throwable exception, int expectedConnectionReleaseCount) {
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
            getServerApi() >> null
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
            getServerApi() >> null
            getWriteConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
            getSessionContext() >> Stub(SessionContext) {
                hasSession() >> true
                hasActiveTransaction() >> false
                getReadConcern() >> ReadConcern.DEFAULT
            }
        }
        def callback = new FutureResultCallback()

        1 * connection.commandAsync(*_) >> { it.last().onResult(null, exception) }
        expectedConnectionReleaseCount * connection.release()

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
