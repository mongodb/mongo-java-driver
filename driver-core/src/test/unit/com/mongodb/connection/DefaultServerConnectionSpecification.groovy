/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.connection

import com.mongodb.MongoNamespace
import com.mongodb.ServerAddress
import com.mongodb.ServerCursor
import com.mongodb.operation.DeleteRequest
import com.mongodb.operation.InsertRequest
import com.mongodb.operation.UpdateRequest
import com.mongodb.operation.WriteRequest
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Specification

import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static java.util.Arrays.asList

class DefaultServerConnectionSpecification extends Specification {
    def namespace = new MongoNamespace('test', 'test')
    def internalConnection = Mock(InternalConnection)
    def executor = Mock(ProtocolExecutor)
    def connection = new DefaultServerConnection(internalConnection, executor)

    def 'should execute insert protocol'() {
        when:
        connection.insert(namespace, true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())))

        then:
        1 * executor.execute(_ as InsertProtocol, internalConnection)
    }

    def 'should execute update protocol'() {
        when:
        connection.update(namespace, true, ACKNOWLEDGED, asList(new UpdateRequest(new BsonDocument(), new BsonDocument(),
                                                                                  WriteRequest.Type.REPLACE)))

        then:
        1 * executor.execute(_ as UpdateProtocol, internalConnection)
    }

    def 'should execute delete protocol'() {
        when:
        connection.delete(namespace, true, ACKNOWLEDGED, asList(new DeleteRequest(new BsonDocument())))

        then:
        1 * executor.execute(_ as DeleteProtocol, internalConnection)
    }

    def 'should execute insert command protocol'() {
        when:
        connection.insertCommand(namespace, true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())))

        then:
        1 * executor.execute(_ as InsertCommandProtocol, internalConnection)
    }

    def 'should execute update command protocol'() {
        when:
        connection.updateCommand(namespace, true, ACKNOWLEDGED, asList(new UpdateRequest(new BsonDocument(), new BsonDocument(),
                                                                                  WriteRequest.Type.REPLACE)))

        then:
        1 * executor.execute(_ as UpdateCommandProtocol, internalConnection)
    }

    def 'should execute delete command protocol'() {
        when:
        connection.deleteCommand(namespace, true, ACKNOWLEDGED, asList(new DeleteRequest(new BsonDocument())))

        then:
        1 * executor.execute(_ as DeleteCommandProtocol, internalConnection)
    }

    def 'should execute command protocol'() {
        when:
        connection.command('test', new BsonDocument('ismaster', new BsonInt32(1)), false, new NoOpFieldNameValidator(),
                           new BsonDocumentCodec())

        then:
        1 * executor.execute(_ as CommandProtocol, internalConnection)
    }

    def 'should execute query protocol'() {
        when:
        connection.query(namespace, new BsonDocument(), null, 0, 0, false, false, false, false, false, false, false,
                         new BsonDocumentCodec())

        then:
        1 * executor.execute(_ as QueryProtocol, internalConnection)
    }

    def 'should execute getmore protocol'() {
        when:
        connection.getMore(namespace, 1L, 1, new BsonDocumentCodec())

        then:
        1 * executor.execute(_ as GetMoreProtocol, internalConnection)
    }

    def 'should execute getmore receive protocol'() {
        when:
        connection.getMoreReceive(new BsonDocumentCodec(), 1)

        then:
        1 * executor.execute(_ as GetMoreReceiveProtocol, internalConnection)
    }

    def 'should execute getmore discard protocol'() {
        when:
        connection.getMoreDiscard(5, 1)

        then:
        1 * executor.execute(_ as GetMoreDiscardProtocol, internalConnection)
    }

    def 'should execute kill cursor protocol'() {
        when:
        connection.killCursor(asList(new ServerCursor(5, new ServerAddress())))
        then:
        1 * executor.execute(_ as KillCursorProtocol, internalConnection)
    }

    def 'should execute insert protocol asynchronously'() {
        when:
        connection.insertAsync(namespace, true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())))

        then:
        1 * executor.executeAsync(_ as InsertProtocol, internalConnection)
    }

    def 'should execute update protocol asynchronously'() {
        when:
        connection.updateAsync(namespace, true, ACKNOWLEDGED, asList(new UpdateRequest(new BsonDocument(), new BsonDocument(),
                                                                                  WriteRequest.Type.REPLACE)))

        then:
        1 * executor.executeAsync(_ as UpdateProtocol, internalConnection)
    }

    def 'should execute delete protocol asynchronously'() {
        when:
        connection.deleteAsync(namespace, true, ACKNOWLEDGED, asList(new DeleteRequest(new BsonDocument())))

        then:
        1 * executor.executeAsync(_ as DeleteProtocol, internalConnection)
    }

    def 'should execute insert command protocol asynchronously'() {
        when:
        connection.insertCommandAsync(namespace, true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())))

        then:
        1 * executor.executeAsync(_ as InsertCommandProtocol, internalConnection)
    }

    def 'should execute update command protocol asynchronously'() {
        when:
        connection.updateCommandAsync(namespace, true, ACKNOWLEDGED, asList(new UpdateRequest(new BsonDocument(), new BsonDocument(),
                                                                                         WriteRequest.Type.REPLACE)))

        then:
        1 * executor.executeAsync(_ as UpdateCommandProtocol, internalConnection)
    }

    def 'should execute delete command protocol asynchronously'() {
        when:
        connection.deleteCommandAsync(namespace, true, ACKNOWLEDGED, asList(new DeleteRequest(new BsonDocument())))

        then:
        1 * executor.executeAsync(_ as DeleteCommandProtocol, internalConnection)
    }

    def 'should execute command protocol asynchronously'() {
        when:
        connection.commandAsync('test', new BsonDocument('ismaster', new BsonInt32(1)), false, new NoOpFieldNameValidator(),
                                new BsonDocumentCodec())

        then:
        1 * executor.executeAsync(_ as CommandProtocol, internalConnection)
    }

    def 'should execute query protocol asynchronously'() {
        when:
        connection.queryAsync(namespace, new BsonDocument(), null, 0, 0, false, false, false, false, false, false, false,
                         new BsonDocumentCodec())

        then:
        1 * executor.executeAsync(_ as QueryProtocol, internalConnection)
    }

    def 'should execute getmore protocol asynchronously'() {
        when:
        connection.getMoreAsync(namespace, 1L, 0, new BsonDocumentCodec())

        then:
        1 * executor.executeAsync(_ as GetMoreProtocol, internalConnection)
    }

    def 'should execute getmore receive protocol asynchronously'() {
        when:
        connection.getMoreReceiveAsync(new BsonDocumentCodec(), 1)

        then:
        1 * executor.executeAsync(_ as GetMoreReceiveProtocol, internalConnection)
    }

    def 'should execute getmore discard protocol asynchronously'() {
        when:
        connection.getMoreDiscardAsync(5, 1)

        then:
        1 * executor.executeAsync(_ as GetMoreDiscardProtocol, internalConnection)
    }

    def 'should execute kill cursor protocol asynchronously'() {
        when:
        connection.killCursorAsync(asList(new ServerCursor(5, new ServerAddress())))
        then:
        1 * executor.executeAsync(_ as KillCursorProtocol, internalConnection)
    }
}