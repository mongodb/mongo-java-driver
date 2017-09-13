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

import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.bulk.DeleteRequest
import com.mongodb.bulk.InsertRequest
import com.mongodb.bulk.UpdateRequest
import com.mongodb.connection.netty.NettyStreamFactory
import com.mongodb.event.CommandStartedEvent
import com.mongodb.event.CommandSucceededEvent
import com.mongodb.internal.connection.NoOpSessionContext
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.codecs.BsonDocumentCodec
import spock.lang.IgnoreIf
import spock.lang.Shared

import static com.mongodb.ClusterFixture.getCredentialList
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.WriteConcern.UNACKNOWLEDGED
import static com.mongodb.bulk.WriteRequest.Type.UPDATE
import static com.mongodb.connection.ProtocolTestHelper.execute

class WriteProtocolCommandEventSpecification extends OperationFunctionalSpecification {
    @Shared
    InternalStreamConnection connection

    def setupSpec() {
        connection = new InternalStreamConnectionFactory(new NettyStreamFactory(SocketSettings.builder().build(), getSslSettings()),
                getCredentialList(), null, null, [], null)
                .create(new ServerId(new ClusterId(), getPrimary()))
        connection.open()
    }

    def cleanupSpec() {
        connection?.close()
    }

    def 'should deliver started and completed command events for a single unacknowledged insert'() {
        given:
        def document = new BsonDocument('_id', new BsonInt32(1))

        def insertRequest = [new InsertRequest(document)]
        def protocol = new InsertProtocol(getNamespace(), true, UNACKNOWLEDGED, insertRequest)
        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        def expectedEvents = [
                new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'insert',
                        new BsonDocument('insert', new BsonString(getCollectionName()))
                                .append('ordered', BsonBoolean.TRUE)
                                .append('writeConcern',
                                new BsonDocument('w', new BsonInt32(0)))
                                .append('documents', new BsonArray(
                                [new BsonDocument('_id', new BsonInt32(1))]))),
                new CommandSucceededEvent(1, connection.getDescription(), 'insert',
                        new BsonDocument('ok', new BsonInt32(1)), 0)]

        when:
        execute(protocol, connection, async)

        then:
        commandListener.eventsWereDelivered(expectedEvents)

        cleanup:
        // force acknowledgement
        new SimpleCommandProtocol(getDatabaseName(), new BsonDocument('drop', new BsonString(getCollectionName())),
                            new NoOpFieldNameValidator(), new BsonDocumentCodec())
                .readPreference(ReadPreference.primary())
                .sessionContext(NoOpSessionContext.INSTANCE)
                .execute(connection)

        where:
        async << [false, true]
    }

    @IgnoreIf({ (serverVersionAtLeast(3, 5) && isSharded()) })
    def 'should deliver started and completed command events for split unacknowledged inserts'() {
        given:
        def binary = new BsonBinary(new byte[15000000])
        def documentOne = new BsonDocument('_id', new BsonInt32(1)).append('b', binary)
        def documentTwo = new BsonDocument('_id', new BsonInt32(2)).append('b', binary)
        def documentThree = new BsonDocument('_id', new BsonInt32(3)).append('b', binary)
        def documentFour = new BsonDocument('_id', new BsonInt32(4)).append('b', binary)

        def insertRequest = [new InsertRequest(documentOne), new InsertRequest(documentTwo),
                             new InsertRequest(documentThree), new InsertRequest(documentFour)]
        def protocol = new InsertProtocol(getNamespace(), true, UNACKNOWLEDGED, insertRequest)
        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        execute(protocol, connection, async)

        then:
        commandListener.eventsWereDelivered([
                new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'insert',
                                        new BsonDocument('insert', new BsonString(getCollectionName()))
                                                .append('ordered', BsonBoolean.TRUE)
                                                .append('writeConcern',
                                                        new BsonDocument('w', new BsonInt32(0)))
                                                .append('documents',
                                                        new BsonArray([documentOne, documentTwo,
                                                                       documentThree]))),
                new CommandSucceededEvent(1, connection.getDescription(), 'insert',
                                          new BsonDocument('ok', new BsonInt32(1)), 0),
                new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'insert',
                                        new BsonDocument('insert', new BsonString(getCollectionName()))
                                                .append('ordered', BsonBoolean.TRUE)
                                                .append('writeConcern',
                                                        new BsonDocument('w', new BsonInt32(0)))
                                                .append('documents', new BsonArray([documentFour]))),
                new CommandSucceededEvent(1, connection.getDescription(), 'insert',
                                          new BsonDocument('ok', new BsonInt32(1)), 0)])

        cleanup:
        // force acknowledgement
        new SimpleCommandProtocol(getDatabaseName(), new BsonDocument('drop', new BsonString(getCollectionName())),
                            new NoOpFieldNameValidator(), new BsonDocumentCodec())
                .readPreference(ReadPreference.primary())
                .sessionContext(NoOpSessionContext.INSTANCE)
                .execute(connection)

        where:
        async << [false, true]
    }

    def 'should deliver started and completed command events for a single unacknowledged update'() {
        given:
        def filter = new BsonDocument('_id', new BsonInt32(1))
        def update = new BsonDocument('$set', new BsonDocument('x', new BsonInt32(1)))
        def updateRequest = [new UpdateRequest(filter, update, UPDATE).multi(true).upsert(true)]
        def protocol = new UpdateProtocol(getNamespace(), true, UNACKNOWLEDGED, updateRequest)
        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        execute(protocol, connection, async)

        then:
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'update',
                                                                     new BsonDocument('update', new BsonString(getCollectionName()))
                                                                             .append('ordered', BsonBoolean.TRUE)
                                                                             .append('writeConcern',
                                                                                     new BsonDocument('w', new BsonInt32(0)))
                                                                             .append('updates', new BsonArray(
                                                                             [new BsonDocument('q', filter)
                                                                                      .append('u', update)
                                                                                      .append('multi', BsonBoolean.TRUE)
                                                                                      .append('upsert', BsonBoolean.TRUE)]))),
                                             new CommandSucceededEvent(1, connection.getDescription(), 'update',
                                                                       new BsonDocument('ok', new BsonInt32(1)), 0)])

        cleanup:
        // force acknowledgement
        new SimpleCommandProtocol(getDatabaseName(), new BsonDocument('drop', new BsonString(getCollectionName())),
                            new NoOpFieldNameValidator(), new BsonDocumentCodec())
                .readPreference(ReadPreference.primary())
                .sessionContext(NoOpSessionContext.INSTANCE)
                .execute(connection)

        where:
        async << [false, true]
    }

    def 'should deliver started and completed command events for a single unacknowledged delete'() {
        given:
        def filter = new BsonDocument('_id', new BsonInt32(1))
        def deleteRequest = [new DeleteRequest(filter).multi(true)]
        def protocol = new DeleteProtocol(getNamespace(), true, UNACKNOWLEDGED, deleteRequest)
        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        def expectedEvents = [
                new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'delete',
                        new BsonDocument('delete', new BsonString(getCollectionName()))
                                .append('ordered', BsonBoolean.TRUE)
                                .append('writeConcern',
                                new BsonDocument('w', new BsonInt32(0)))
                                .append('deletes', new BsonArray(
                                [new BsonDocument('q', filter)
                                         .append('limit', new BsonInt32(0))]))),
                new CommandSucceededEvent(1, connection.getDescription(), 'delete',
                        new BsonDocument('ok', new BsonInt32(1)), 0)]

        when:
        execute(protocol, connection, async)

        then:
        commandListener.eventsWereDelivered(expectedEvents)

        where:
        async << [false, true]
    }

    def 'should not deliver any events if encoding fails'() {
        given:
        def insertRequest = [new InsertRequest(new BsonDocument('$set', new BsonInt32(1)))]
        def protocol = new InsertProtocol(getNamespace(), true, UNACKNOWLEDGED, insertRequest)
        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        def expectedEvents = []

        when:
        execute(protocol, connection, async)

        then:
        thrown(IllegalArgumentException)
        commandListener.eventsWereDelivered(expectedEvents)

        where:
        async << [false, true]
    }
}
