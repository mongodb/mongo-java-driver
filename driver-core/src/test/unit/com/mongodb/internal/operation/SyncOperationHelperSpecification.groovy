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
import com.mongodb.internal.binding.ConnectionSource
import com.mongodb.internal.binding.ReadBinding
import com.mongodb.internal.binding.WriteBinding
import com.mongodb.internal.connection.Connection
import com.mongodb.internal.session.SessionContext
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonDocument
import org.bson.BsonNull
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.Decoder
import spock.lang.Specification

import static com.mongodb.ClusterFixture.getOperationContext
import static com.mongodb.ReadPreference.primary
import static com.mongodb.internal.operation.OperationUnitSpecification.getMaxWireVersionForServerVersion
import static com.mongodb.internal.operation.SyncOperationHelper.CommandReadTransformer
import static com.mongodb.internal.operation.SyncOperationHelper.CommandWriteTransformer
import static com.mongodb.internal.operation.SyncOperationHelper.executeCommand
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableRead
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableWrite

class SyncOperationHelperSpecification extends Specification {

    def 'should set read preference to primary to false when using WriteBinding'() {
        given:
        def dbName = 'db'
        def command = new BsonDocument()
        def decoder = Stub(Decoder)
        def connection = Mock(Connection)
        def function = Stub(CommandWriteTransformer)
        def connectionSource = Stub(ConnectionSource) {
            getConnection(_) >> connection
        }
        def writeBinding = Stub(WriteBinding) {
            getWriteConnectionSource(_) >> connectionSource
        }
        def connectionDescription = Stub(ConnectionDescription)

        when:
        executeCommand(writeBinding, getOperationContext(), dbName, command, decoder, function)

        then:
        _ * connection.getDescription() >> connectionDescription
        1 * connection.command(dbName, command, _, primary(), decoder, _) >> new BsonDocument()
        1 * connection.release()
    }

    def 'should retry with retryable exception'() {
        given:
        def operationContext = getOperationContext()
                .withSessionContext(Stub(SessionContext) {
                    hasSession() >> true
                    hasActiveTransaction() >> false
                    getReadConcern() >> ReadConcern.DEFAULT
                })
        def dbName = 'db'
        def command = BsonDocument.parse('''{findAndModify: "coll", query: {a: 1}, new: false, update: {$inc: {a :1}}, txnNumber: 1}''')
        def commandCreator = { csot, serverDescription, connectionDescription -> command }
        def decoder = new BsonDocumentCodec()
        def results = [
            BsonDocument.parse('{ok: 1.0, writeConcernError: {code: 91, errmsg: "Replication is being shut down"}}'),
            BsonDocument.parse('{ok: 1.0, writeConcernError: {code: -1, errmsg: "UnknownError"}}')] as Queue
        def connection = Mock(Connection) {
            _ * release()
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> getMaxWireVersionForServerVersion([4, 0, 0])
                getServerType() >> ServerType.REPLICA_SET_PRIMARY
            }
        }
        def connectionSource = Stub(ConnectionSource) {
            _ * getConnection(_) >> connection
            _ * getServerDescription() >> Stub(ServerDescription) {
                getLogicalSessionTimeoutMinutes() >> 1
            }
        }
        def writeBinding = Stub(WriteBinding) {
            getWriteConnectionSource(_) >> connectionSource
        }

        when:
        executeRetryableWrite(writeBinding, operationContext, dbName, primary(),
                NoOpFieldNameValidator.INSTANCE, decoder, commandCreator, FindAndModifyHelper.transformer())
                { cmd -> cmd }

        then:
        2 * connection.command(dbName, command, _, primary(), decoder, _) >> { results.poll() }

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == -1
    }

    def 'should use the ConnectionSource readPreference'() {
        given:
        def dbName = 'db'
        def command = new BsonDocument('fakeCommandName', BsonNull.VALUE)
        def commandCreator = { csot, serverDescription, connectionDescription -> command }
        def decoder = Stub(Decoder)
        def function = Stub(CommandReadTransformer)
        def connection = Mock(Connection)
        def connectionSource = Stub(ConnectionSource) {
            getConnection(_) >> connection
            getReadPreference() >> readPreference
        }
        def readBinding = Stub(ReadBinding) {
            getReadConnectionSource(_) >> connectionSource
        }
        def connectionDescription = Stub(ConnectionDescription)

        when:
        executeRetryableRead(readBinding, getOperationContext(), dbName, commandCreator, decoder, function, false)

        then:
        _ * connection.getDescription() >> connectionDescription
        1 * connection.command(dbName, command, _, readPreference, decoder, _) >> new BsonDocument()
        1 * connection.release()

        where:
        readPreference << [primary(), ReadPreference.secondary()]
    }

}
