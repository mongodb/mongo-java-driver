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

import com.mongodb.MongoWriteConcernException
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import com.mongodb.internal.async.SingleResultCallback
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.binding.AsyncReadBinding
import com.mongodb.internal.binding.AsyncWriteBinding
import com.mongodb.internal.connection.AsyncConnection
import com.mongodb.internal.session.SessionContext
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonDocument
import org.bson.BsonNull
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.Decoder
import spock.lang.Specification

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.ReadPreference.primary
import static com.mongodb.internal.operation.AsyncOperationHelper.CommandReadTransformerAsync
import static com.mongodb.internal.operation.AsyncOperationHelper.executeCommandAsync
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableWriteAsync
import static com.mongodb.internal.operation.OperationUnitSpecification.getMaxWireVersionForServerVersion

class AsyncOperationHelperSpecification extends Specification {

    def 'should retry with retryable exception async'() {
        given:
        def dbName = 'db'
        def command = BsonDocument.parse('''{findAndModify: "coll", query: {a: 1}, new: false, update: {$inc: {a :1}}, txnNumber: 1}''')
        def serverDescription = Stub(ServerDescription)
        def connectionDescription = Stub(ConnectionDescription) {
            getMaxWireVersion() >> getMaxWireVersionForServerVersion([4, 0, 0])
            getServerType() >> ServerType.REPLICA_SET_PRIMARY
        }
        def commandCreator = { csot, serverDesc, connectionDesc -> command }
        def callback = new SingleResultCallback() {
            def result
            def throwable
            @Override
            void onResult(final Object result, final Throwable t) {
                this.result = result
                this.throwable = t
            }
        }
        def decoder = new BsonDocumentCodec()
        def results = [
                BsonDocument.parse('{ok: 1.0, writeConcernError: {code: 91, errmsg: "Replication is being shut down"}}'),
                BsonDocument.parse('{ok: 1.0, writeConcernError: {code: -1, errmsg: "UnknownError"}}')] as Queue

        def connection = Mock(AsyncConnection) {
            _ * getDescription() >> connectionDescription
        }

        def operationContext = OPERATION_CONTEXT.withSessionContext(
                Stub(SessionContext) {
                    hasSession() >> true
                    hasActiveTransaction() >> false
                    getReadConcern() >> ReadConcern.DEFAULT
                })
        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
            getServerDescription() >> serverDescription
            getOperationContext() >> operationContext
        }
        def asyncWriteBinding = Stub(AsyncWriteBinding) {
            getWriteConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
            getOperationContext() >> operationContext
        }

        when:
        executeRetryableWriteAsync(asyncWriteBinding, dbName, primary(),
                NoOpFieldNameValidator.INSTANCE, decoder, commandCreator, FindAndModifyHelper.asyncTransformer(),
                { cmd -> cmd }, callback)

        then:
        2 * connection.commandAsync(dbName, command, _, primary(), decoder, *_) >> { it.last().onResult(results.poll(), null) }

        then:
        callback.throwable instanceof MongoWriteConcernException
        callback.throwable.writeConcernError.code == -1
    }

    def 'should set read preference to primary when using AsyncWriteBinding'() {
        given:
        def dbName = 'db'
        def command = new BsonDocument()
        def callback = Stub(SingleResultCallback)
        def connection = Mock(AsyncConnection)
        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        def asyncWriteBinding = Stub(AsyncWriteBinding) {
            getWriteConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        }
        def connectionDescription = Stub(ConnectionDescription)

        when:
        executeCommandAsync(asyncWriteBinding, dbName, command, connection, { t, conn -> t }, callback)

        then:
        _ * connection.getDescription() >> connectionDescription
        1 * connection.commandAsync(dbName, command, _, primary(), *_) >> { it.last().onResult(1, null) }
//        1 * connection.release()
    }

    def 'should use the AsyncConnectionSource readPreference'() {
        given:
        def dbName = 'db'
        def command = new BsonDocument('fakeCommandName', BsonNull.VALUE)
        def commandCreator = { csot, serverDescription, connectionDescription -> command }
        def decoder = Stub(Decoder)
        def callback = Stub(SingleResultCallback)
        def function = Stub(CommandReadTransformerAsync)
        def connection = Mock(AsyncConnection)
        def connectionSource = Stub(AsyncConnectionSource) {
            getOperationContext() >> OPERATION_CONTEXT
            getConnection(_) >> { it[0].onResult(connection, null) }
            getReadPreference() >> readPreference
        }
        def asyncReadBinding = Stub(AsyncReadBinding) {
            getOperationContext() >> OPERATION_CONTEXT
            getReadConnectionSource(_)  >> { it[0].onResult(connectionSource, null) }
        }
        def connectionDescription = Stub(ConnectionDescription)

        when:
        executeRetryableReadAsync(asyncReadBinding, dbName, commandCreator, decoder, function, false, callback)

        then:
        _ * connection.getDescription() >> connectionDescription
        1 * connection.commandAsync(dbName, command, _, readPreference, decoder, *_) >> { it.last().onResult(1, null) }
        1 * connection.release()

        where:
        readPreference << [primary(), ReadPreference.secondary()]
    }

}
