/*
 * Copyright 2015 MongoDB, Inc.
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
import com.mongodb.Function
import com.mongodb.MongoCommandException
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
import com.mongodb.connection.ClusterId
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerId
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.codecs.Decoder
import spock.lang.Specification

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

    def 'should set slaveOK to false when using WriteBinding'() {
        given:
        def dbName = "db"
        def command = new BsonDocument()
        def decoder = Stub(Decoder)
        def writeBinding = Mock(WriteBinding)
        def function = Mock(Function)
        def connectionSource = Mock(ConnectionSource)
        def connection = Mock(Connection)
        def connectionDescription = new ConnectionDescription(new ServerId(new ClusterId("cluster"), new ServerAddress("localhost")))

        when:
        executeWrappedCommandProtocol(dbName, command, decoder, writeBinding, function)

        then:
        1 * writeBinding.getWriteConnectionSource() >> connectionSource

        then:
        1 * connectionSource.getConnection() >> connection
        1 * connection.getDescription() >> connectionDescription

        then:
        1 * connection.command(dbName, command, false, _, decoder)

        then:
        1 * connection.release()
        1 * function.apply(_)
        1 * connectionSource.release()
    }

    def 'should use the ReadBindings readPreference to set slaveOK'() {
        given:
        def dbName = "db"
        def command = new BsonDocument()
        def decoder = Stub(Decoder)
        def readBinding = Mock(ReadBinding)
        def readPreference = Mock(ReadPreference)
        def function = Mock(Function)
        def connectionSource = Mock(ConnectionSource)
        def connection = Mock(Connection)
        def connectionDescription = new ConnectionDescription(new ServerId(new ClusterId("cluster"), new ServerAddress("localhost")))

        when:
        executeWrappedCommandProtocol(dbName, command, decoder, readBinding, function)

        then:
        1 * readBinding.getReadConnectionSource() >> connectionSource
        1 * readBinding.getReadPreference() >> readPreference

        then:
        1 * connectionSource.getConnection() >> connection
        1 * connection.getDescription() >> connectionDescription

        then:
        1 * readPreference.slaveOk >> true
        1 * connection.command(dbName, command, true, _, decoder)

        then:
        1 * connection.release()
        1 * function.apply(_)
        1 * connectionSource.release()
    }

    def 'should set slaveOK to false when using AsyncWriteBinding'() {
        given:
        def dbName = "db"
        def command = new BsonDocument()
        def decoder = Stub(Decoder)
        def asyncWriteBinding = Mock(AsyncWriteBinding)
        def function = Mock(Function)
        def callback = Stub(SingleResultCallback)
        def connectionSource = Mock(AsyncConnectionSource)
        def connection = Mock(AsyncConnection)
        def connectionDescription = new ConnectionDescription(new ServerId(new ClusterId("cluster"), new ServerAddress("localhost")))

        when:
        executeWrappedCommandProtocolAsync(dbName, command, decoder, asyncWriteBinding, function, callback)

        then:
        1 * asyncWriteBinding.getWriteConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        1 * connectionSource.getConnection(_) >> { it[0].onResult(connection, null)}
        1 * connection.getDescription() >> connectionDescription
        1 * connection.commandAsync(dbName, command, false, _, decoder, _) >> { it[5].onResult(1, null)}
        1 * connection.release()
        1 * connectionSource.release()
    }

    def 'should use the AsyncReadBindings readPreference to set slaveOK'() {
        given:
        def dbName = "db"
        def command = new BsonDocument()
        def decoder = Stub(Decoder)
        def asyncReadBinding = Mock(AsyncReadBinding)
        def readPreference = Mock(ReadPreference)
        def function = Mock(Function)
        def callback = Stub(SingleResultCallback)
        def connectionSource = Mock(AsyncConnectionSource)
        def connection = Mock(AsyncConnection)
        def connectionDescription = new ConnectionDescription(new ServerId(new ClusterId("cluster"), new ServerAddress("localhost")))

        when:
        executeWrappedCommandProtocolAsync(dbName, command, decoder, asyncReadBinding, function, callback)

        then:
        1 * asyncReadBinding.getReadPreference() >> readPreference

        then:
        1 * asyncReadBinding.getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        1 * connectionSource.getConnection(_) >> { it[0].onResult(connection, null)}
        1 * connection.getDescription() >> connectionDescription
        1 * readPreference.slaveOk >> true
        1 * connection.commandAsync(dbName, command, true, _, decoder, _) >> { it[5].onResult(1, null)}
        1 * connection.release()
        1 * connectionSource.release()
    }

}
