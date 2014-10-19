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

package com.mongodb.protocol

import category.Async
import com.mongodb.MongoException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ServerAddress
import com.mongodb.WriteConcern
import com.mongodb.connection.MessageHelper
import com.mongodb.connection.TestConnection
import com.mongodb.operation.InsertRequest
import com.mongodb.protocol.message.NoOpFieldNameValidator
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf
import spock.lang.Shared
import spock.lang.Unroll

import static java.util.concurrent.TimeUnit.SECONDS

class ProtocolServerStateErrorSpecification extends OperationFunctionalSpecification {

    @Shared
    def commandProtocol = new CommandProtocol<Document>(getDatabaseName(), new BsonDocument('count', new BsonString(getCollectionName())),
                                                        true, new NoOpFieldNameValidator(), new DocumentCodec())

    @Shared
    def queryProtocol = new QueryProtocol<Document>(getNamespace(), 0, 0, new BsonDocument(), null, new DocumentCodec())

    @Shared
    def writeProtocol = new InsertProtocol(getNamespace(), true, WriteConcern.ACKNOWLEDGED, [new InsertRequest(new BsonDocument())]);

    @Unroll
    def 'should call unexpectedServerState on not master errors from #commandType'() {
        when:
        def connection = new TestConnection(new ServerAddress())
        connection.enqueueReply(message)

        then:
        !connection.unexpectedServerStateCalled()

        when:
        protocol.execute(connection)

        then:
        thrown(MongoException)
        connection.unexpectedServerStateCalled()

        where:
        commandType                   | message                                                                        | protocol
        'commandProtocol recovering'  | MessageHelper.buildSuccessfulReply('{ok: 0, errmsg: "node is recovering"}')    | commandProtocol
        'commandProtocol not master'  | MessageHelper.buildSuccessfulReply('{ok: 0, errmsg: "not master"}')            | commandProtocol
        'queryProtocol not master'    | MessageHelper.buildFailedReply('{$err: "not master or secondary; cannot ' +
                                                                     'currently read from this replSet member"}')      | queryProtocol
        'queryProtocol slaveOk=false' | MessageHelper.buildFailedReply('{$err: "not master and slaveOk=false"}')       | queryProtocol
        'writeProtocol getLastError'  | MessageHelper.buildSuccessfulReply('{ok: 1, err: "not master"}')               | writeProtocol
        'writeProtocol not master'    | MessageHelper.buildSuccessfulReply('{ok: 0, errmsg: "not master"}')            | writeProtocol
    }

    @Category(Async)
    @IgnoreIf({ javaVersion < 1.7 })
    @Unroll
    def 'should call unexpectedServerState on not master errors asynchronously #commandType'() {
        when:
        def connection = new TestConnection(new ServerAddress())
        connection.enqueueReply(message)

        then:
        !connection.unexpectedServerStateCalled()

        when:
        protocol.executeAsync(connection).get(10, SECONDS)

        then:
        thrown(MongoException)
        connection.unexpectedServerStateCalled()

        where:
        commandType                   | message                                                                        | protocol
        'commandProtocol recovering'  | MessageHelper.buildSuccessfulReply('{ok: 0, errmsg: "node is recovering"}')    | commandProtocol
        'commandProtocol not master'  | MessageHelper.buildSuccessfulReply('{ok: 0, errmsg: "not master"}')            | commandProtocol
        'queryProtocol not master'    | MessageHelper.buildFailedReply('{$err: "not master or secondary; cannot ' +
                                                                       'currently read from this replSet member"}')    | queryProtocol
        'queryProtocol slaveOk=false' | MessageHelper.buildFailedReply('{$err: "not master and slaveOk=false"}')       | queryProtocol
        'writeProtocol getLastError'  | MessageHelper.buildSuccessfulReply('{ok: 1, err: "not master"}')               | writeProtocol
        'writeProtocol not master'    | MessageHelper.buildSuccessfulReply('{ok: 0, errmsg: "not master"}')            | writeProtocol
    }
}
