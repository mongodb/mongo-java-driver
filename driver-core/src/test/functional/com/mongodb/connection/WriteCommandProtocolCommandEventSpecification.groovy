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
 *
 *
 */

package com.mongodb.connection

import com.mongodb.MongoBulkWriteException
import com.mongodb.MongoCommandException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ServerAddress
import com.mongodb.bulk.DeleteRequest
import com.mongodb.bulk.InsertRequest
import com.mongodb.bulk.UpdateRequest
import com.mongodb.bulk.WriteRequest
import com.mongodb.connection.netty.NettyStreamFactory
import com.mongodb.event.CommandFailedEvent
import com.mongodb.event.CommandStartedEvent
import com.mongodb.event.CommandSucceededEvent
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import spock.lang.IgnoreIf
import spock.lang.Shared

import static com.mongodb.ClusterFixture.getCredentialList
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.connection.MessageHelper.buildSuccessfulReply
import static com.mongodb.connection.ProtocolTestHelper.execute

@IgnoreIf({ !serverVersionAtLeast([2, 6, 0]) })
class WriteCommandProtocolCommandEventSpecification extends OperationFunctionalSpecification {
    @Shared
    InternalStreamConnection connection;

    def setupSpec() {
        connection = new InternalStreamConnectionFactory(new NettyStreamFactory(SocketSettings.builder().build(), getSslSettings()),
                getCredentialList(), new NoOpConnectionListener(), null, null)
                .create(new ServerId(new ClusterId(), getPrimary()))
        connection.open();
    }

    def cleanupSpec() {
        connection?.close()
    }

    def 'should deliver start and completed insert command events'() {
        given:
        def document = new BsonDocument('_id', new BsonInt32(1))

        def insertRequest = [new InsertRequest(document)]
        def protocol = new InsertCommandProtocol(getNamespace(), true, ACKNOWLEDGED, null, insertRequest)

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        execute(protocol, connection, async)

        then:
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'insert',
                                                                     new BsonDocument('insert', new BsonString(getCollectionName()))
                                                                             .append('ordered', BsonBoolean.TRUE)
                                                                             .append('documents', new BsonArray(
                                                                             [new BsonDocument('_id', new BsonInt32(1))]))),
                                             new CommandSucceededEvent(1, connection.getDescription(), 'insert',
                                                                       new BsonDocument('ok', new BsonInt32(1))
                                                                               .append('n', new BsonInt32(1)),
                                                                       0)])

        where:
        async << [false, true]
    }

    def 'should deliver start and completed delete command events'() {
        given:
        def filter = new BsonDocument('_id', new BsonInt32(1))

        def deleteRequest = [new DeleteRequest(filter)]
        def protocol = new DeleteCommandProtocol(getNamespace(), true, ACKNOWLEDGED, deleteRequest)

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        execute(protocol, connection, async)

        then:
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'delete',
                                                                     new BsonDocument('delete', new BsonString(getCollectionName()))
                                                                             .append('ordered', BsonBoolean.TRUE)
                                                                             .append('deletes', new BsonArray(
                                                                             [new BsonDocument('q', filter)
                                                                                      .append('limit', new BsonInt32(0))]))),
                                             new CommandSucceededEvent(1, connection.getDescription(), 'delete',
                                                                       new BsonDocument('ok', new BsonInt32(1))
                                                                               .append('n', new BsonInt32(0)),
                                                                       0)])

        where:
        async << [false, true]
    }

    def 'should deliver start and completed update command events'() {
        given:
        def filter = new BsonDocument('_id', new BsonInt32(1))
        def update = new BsonDocument('$set', new BsonDocument('x', new BsonInt32(1)))
        def updateRequest = [new UpdateRequest(filter, update, WriteRequest.Type.UPDATE)]
        def protocol = new UpdateCommandProtocol(getNamespace(), true, ACKNOWLEDGED, null, updateRequest)

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        execute(protocol, connection, async)

        then:
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'update',
                                                                     new BsonDocument('update', new BsonString(getCollectionName()))
                                                                             .append('ordered', BsonBoolean.TRUE)
                                                                             .append('updates', new BsonArray(
                                                                             [new BsonDocument('q', filter)
                                                                                      .append('u', update)
                                                                                      .append('multi', BsonBoolean.TRUE)]))),
                                             new CommandSucceededEvent(1, connection.getDescription(), 'update',
                                                                       new BsonDocument('ok', new BsonInt32(1))
                                                                               .append('nModified', new BsonInt32(0))
                                                                               .append('n', new BsonInt32(0)),
                                                                       0)])

        where:
        async << [false, true]
    }

    def 'should deliver start and completed command events when there is a write error'() {
        given:
        def document = new BsonDocument('_id', new BsonInt32(1))

        def insertRequest = [new InsertRequest(document)]
        def protocol = new InsertCommandProtocol(getNamespace(), true, ACKNOWLEDGED, null, insertRequest)
        protocol.execute(connection)  // insert here, to force a duplicate key error on the second time

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        execute(protocol, connection, async)

        then:
        def e = thrown(MongoBulkWriteException)
        def writeError = e.getWriteErrors()[0]
        def writeErrorDocument = new BsonDocument('index', new BsonInt32(writeError.getIndex()))
                .append('code', new BsonInt32(writeError.getCode()))
                .append('errmsg', new BsonString(writeError.getMessage()))
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'insert',
                                                                     new BsonDocument('insert', new BsonString(getCollectionName()))
                                                                             .append('ordered', BsonBoolean.TRUE)
                                                                             .append('documents', new BsonArray(
                                                                             [new BsonDocument('_id', new BsonInt32(1))]))),
                                             new CommandSucceededEvent(1, connection.getDescription(), 'insert',
                                                                       new BsonDocument('ok', new BsonInt32(1))
                                                                               .append('n', new BsonInt32(0))
                                                                               .append('writeErrors', new BsonArray([writeErrorDocument])),
                                                                       0)])

        where:
        async << [false, true]
    }

    def 'should deliver start and failed command events'() {
        given:
        // need a test connection to generate an ok : 0 response
        def connection = new TestInternalConnection(new ServerId(new ClusterId(), new ServerAddress('localhost', 27017)));
        connection.enqueueReply(buildSuccessfulReply('{ ok : 0, errmsg : "some error"}'))
        def filter = new BsonDocument('_id', new BsonInt32(1))

        def deleteRequest = [new DeleteRequest(filter)]
        def protocol = new DeleteCommandProtocol(getNamespace(), true, ACKNOWLEDGED, deleteRequest)

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        execute(protocol, connection, async)

        then:
        def e = thrown(MongoCommandException)
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'delete',
                                                                     new BsonDocument('delete', new BsonString(getCollectionName()))
                                                                             .append('ordered', BsonBoolean.TRUE)
                                                                             .append('deletes', new BsonArray(
                                                                             [new BsonDocument('q', filter)
                                                                                      .append('limit', new BsonInt32(0))]))),
                                             new CommandFailedEvent(1, connection.getDescription(),
                                                                    'delete', 0, e)])

        where:
        async << [false, true]
    }
}
