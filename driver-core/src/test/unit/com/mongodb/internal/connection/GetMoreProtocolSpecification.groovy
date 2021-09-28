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

package com.mongodb.internal.connection

import com.mongodb.MongoCursorNotFoundException
import com.mongodb.MongoNamespace
import com.mongodb.MongoQueryException
import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ServerId
import com.mongodb.internal.IgnorableRequestContext
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Specification

import static com.mongodb.internal.connection.MessageHelper.buildReply
import static com.mongodb.internal.connection.ProtocolTestHelper.execute

// unit test failure cases that are difficult to reproduce in integration tests
class GetMoreProtocolSpecification extends Specification {
    def protocol = new GetMoreProtocol(new MongoNamespace('test.test'), 42L, 0, new BsonDocumentCodec(), IgnorableRequestContext.INSTANCE)
    def connection = new TestInternalConnection(new ServerId(new ClusterId(), new ServerAddress()))

    def 'when query failure bit is set then MongoQueryException should be generated'() {
        given:
        connection.enqueueReply(buildReply(0, '{ok : 0}', 2))

        when:
        execute(protocol, connection, async)

        then:
        thrown(MongoQueryException)

        where:
        async << [false, true]
    }

    def 'when cursor not found bit is set then MongoCursorNotFoundException should be generated'() {
        given:
        connection.enqueueReply(buildReply(0, '{ok : 0}', 1))

        when:
        execute(protocol, connection, async)

        then:
        thrown(MongoCursorNotFoundException)

        where:
        async << [false, true]
    }
}
