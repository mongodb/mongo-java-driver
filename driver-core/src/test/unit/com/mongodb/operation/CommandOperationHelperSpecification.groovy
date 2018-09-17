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

package com.mongodb.operation

import com.mongodb.MongoCommandException
import com.mongodb.MongoWriteConcernException
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.async.SingleResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.binding.AsyncWriteBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.binding.WriteBinding
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import com.mongodb.connection.ServerVersion
import com.mongodb.internal.validator.NoOpFieldNameValidator
import com.mongodb.session.SessionContext
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.Decoder
import spock.lang.Specification

import static com.mongodb.ReadPreference.primary
import static com.mongodb.operation.CommandOperationHelper.executeRetryableCommand
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync
import static com.mongodb.operation.CommandOperationHelper.isNamespaceError
import static com.mongodb.operation.CommandOperationHelper.rethrowIfNotNamespaceError

class CommandOperationHelperSpecification extends Specification {

    def 'should be a namespace error if Throwable is a MongoCommandException and error code is 26'() {
        expect:
        isNamespaceError(new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE)
                                                           .append('code', new BsonInt32(26)),
                                                   new ServerAddress()))
    }

    def 'should be a namespace error if Throwable is a MongoCommandException and error message contains "ns not found"'() {
        expect:
        isNamespaceError(new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE)
                                                           .append('errmsg', new BsonString('the ns not found here')),
                                                   new ServerAddress()))
    }

    def 'should not be a namespace error if Throwable is a MongoCommandException and error message does not contain "ns not found"'() {
        expect:
        !isNamespaceError(new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE)
                                                           .append('errmsg', new BsonString('some other error')),
                                                   new ServerAddress()))
    }

    def 'should not be a namespace error should return false if Throwable is not a MongoCommandException'() {
        expect:
        !isNamespaceError(new NullPointerException())
    }

    def 'should rethrow if not namespace error'() {
        when:
        rethrowIfNotNamespaceError(new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE)
                                                                     .append('errmsg', new BsonString('some other error')),
                                                             new ServerAddress()))

        then:
        thrown(MongoCommandException)

        when:
        rethrowIfNotNamespaceError(new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE)
                                                                     .append('errmsg', new BsonString('some other error')),
                                                             new ServerAddress()), 'some value')

        then:
        thrown(MongoCommandException)
    }

    def 'should not rethrow if namespace error'() {
        when:
        rethrowIfNotNamespaceError(new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE)
                                                                     .append('code', new BsonInt32(26)),
                                                             new ServerAddress()))

        then:
        true
    }

    def 'should return default value if not namespace error'() {
        expect:
        rethrowIfNotNamespaceError(new MongoCommandException(new BsonDocument('ok', BsonBoolean.FALSE)
                                                                     .append('code', new BsonInt32(26)),
                                                             new ServerAddress()), 'some value') == 'some value'
    }

    def 'should set read preference to primary to false when using WriteBinding'() {
        given:
        def dbName = 'db'
        def command = new BsonDocument()
        def decoder = Stub(Decoder)
        def connection = Mock(Connection)
        def function = Stub(CommandOperationHelper.CommandTransformer)
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> connection
        }
        def writeBinding = Stub(WriteBinding) {
            getWriteConnectionSource() >> connectionSource
        }
        def connectionDescription = Stub(ConnectionDescription)

        when:
        executeWrappedCommandProtocol(writeBinding, dbName, command, decoder, function)

        then:
        _ * connection.getDescription() >> connectionDescription
        1 * connection.command(dbName, command, _, primary(), decoder, _)
        1 * connection.release()
    }

    def 'should retry with retryable exception'() {
        given:
        def dbName = 'db'
        def command = BsonDocument.parse('''{findAndModify: "coll", query: {a: 1}, new: false, update: {$inc: {a :1}}, txnNumber: 1}''')
        def commandCreator = { serverDescription, connectionDescription -> command }
        def decoder = new BsonDocumentCodec()
        def results = [
            BsonDocument.parse('{ok: 1.0, writeConcernError: {code: 91, errmsg: "Replication is being shut down"}}'),
            BsonDocument.parse('{ok: 1.0, writeConcernError: {code: -1, errmsg: "UnknownError"}}')] as Queue
        def connection = Mock(Connection) {
            _ * release()
            _ * getDescription() >> Stub(ConnectionDescription) {
                getServerVersion() >> new ServerVersion([4, 0, 0])
                getServerType() >> ServerType.REPLICA_SET_PRIMARY
            }
        }
        def connectionSource = Stub(ConnectionSource) {
            _ * getConnection() >> connection
            _ * getServerDescription() >> Stub(ServerDescription) {
                getLogicalSessionTimeoutMinutes() >> 1
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

        when:
        executeRetryableCommand(writeBinding, dbName, primary(), new NoOpFieldNameValidator(), decoder, commandCreator,
                FindAndModifyHelper.transformer())

        then:
        2 * connection.command(dbName, command, _, primary(), decoder, _) >> { results.poll() }

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 91
    }

    def 'should retry with retryable exception async'() {
        given:
        def dbName = 'db'
        def command = BsonDocument.parse('''{findAndModify: "coll", query: {a: 1}, new: false, update: {$inc: {a :1}}, txnNumber: 1}''')
        def commandCreator = { serverDescription, connectionDescription -> command }
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
            _ * getDescription() >> Stub(ConnectionDescription) {
                getServerVersion() >> new ServerVersion([4, 0, 0])
                getServerType() >> ServerType.REPLICA_SET_PRIMARY
            }
        }

        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
            _ * getServerDescription() >> Stub(ServerDescription) {
                getLogicalSessionTimeoutMinutes() >> 1
            }
        }
        def asyncWriteBinding = Stub(AsyncWriteBinding) {
            getWriteConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
            getSessionContext() >> Stub(SessionContext) {
                hasSession() >> true
                hasActiveTransaction() >> false
                getReadConcern() >> ReadConcern.DEFAULT
            }
        }

        when:
        executeRetryableCommand(asyncWriteBinding, dbName, primary(), new NoOpFieldNameValidator(), decoder, commandCreator,
                FindAndModifyHelper.transformer(), callback)

        then:
        2 * connection.commandAsync(dbName, command, _, primary(), decoder, _, _) >> { it.last().onResult(results.poll(), null) }

        then:
        callback.throwable instanceof MongoWriteConcernException
        callback.throwable.writeConcernError.code == 91
    }

    def 'should use the ReadBindings readPreference'() {
        given:
        def dbName = 'db'
        def command = new BsonDocument()
        def decoder = Stub(Decoder)
        def function = Stub(CommandOperationHelper.CommandTransformer)
        def connection = Mock(Connection)
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> connection
        }
        def readBinding = Stub(ReadBinding) {
            getReadConnectionSource() >> connectionSource
            getReadPreference() >> readPreference
        }
        def connectionDescription = Stub(ConnectionDescription)

        when:
        executeWrappedCommandProtocol(readBinding, dbName, command, decoder, function)

        then:
        _ * connection.getDescription() >> connectionDescription
        1 * connection.command(dbName, command, _, readPreference, decoder, _)
        1 * connection.release()

        where:
        readPreference << [primary(), ReadPreference.secondary()]
    }

    def 'should set read preference to primary to false when using AsyncWriteBinding'() {
        given:
        def dbName = 'db'
        def command = new BsonDocument()
        def decoder = Stub(Decoder)
        def callback = Stub(SingleResultCallback)
        def function = Stub(CommandOperationHelper.CommandTransformer)
        def connection = Mock(AsyncConnection)
        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        def asyncWriteBinding = Stub(AsyncWriteBinding) {
            getWriteConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        }
        def connectionDescription = Stub(ConnectionDescription)

        when:
        executeWrappedCommandProtocolAsync(asyncWriteBinding, dbName, command, decoder, function, callback)

        then:
        _ * connection.getDescription() >> connectionDescription
        1 * connection.commandAsync(dbName, command, _, primary(), decoder, _, _) >> { it[6].onResult(1, null) }
        1 * connection.release()
    }

    def 'should use the AsyncReadBindings readPreference'() {
        given:
        def dbName = 'db'
        def command = new BsonDocument()
        def decoder = Stub(Decoder)
        def callback = Stub(SingleResultCallback)
        def function = Stub(CommandOperationHelper.CommandTransformer)
        def connection = Mock(AsyncConnection)
        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        def asyncReadBinding = Stub(AsyncReadBinding) {
            getReadConnectionSource(_)  >> { it[0].onResult(connectionSource, null) }
            getReadPreference() >> readPreference
        }
        def connectionDescription = Stub(ConnectionDescription)

        when:
        executeWrappedCommandProtocolAsync(asyncReadBinding, dbName, command, decoder, function, callback)

        then:
        _ * connection.getDescription() >> connectionDescription
        1 * connection.commandAsync(dbName, command, _, readPreference, decoder, _, _) >> { it[6].onResult(1, null) }
        1 * connection.release()

        where:
        readPreference << [primary(), ReadPreference.secondary()]
    }

}
