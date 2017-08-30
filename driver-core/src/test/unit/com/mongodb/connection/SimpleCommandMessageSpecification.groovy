/*
 * Copyright 2017 MongoDB, Inc.
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
 */

package com.mongodb.connection

import com.mongodb.ReadPreference
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.ByteBufNIO
import org.bson.codecs.BsonDocumentCodec
import org.bson.io.BasicOutputBuffer
import spock.lang.Specification

import java.nio.ByteBuffer

class SimpleCommandMessageSpecification extends Specification {
    def 'should encode command message with OP_MSG'() {
        given:
        def message = new SimpleCommandMessage('db.test', new BsonDocument('count', new BsonString('test')),
                readPreference,
                MessageSettings.builder()
                        .serverVersion(new ServerVersion(3, 6))
                        .build())
        def output = new BasicOutputBuffer()

        when:
        message.encode(output, sessionContext)

        then:
        def byteBuf = new ByteBufNIO(ByteBuffer.wrap(output.toByteArray()))
        def messageHeader = new MessageHeader(byteBuf, 512)
        messageHeader.opCode == OpCode.OP_MSG.value
        messageHeader.requestId == RequestMessage.currentGlobalId - 1
        messageHeader.responseTo == 0

        def expectedCommandDocument = new BsonDocument()
                .append('count', new BsonString('test'))
                .append('$db', new BsonString('db'))

        if (sessionContext.hasSession()) {
            if (sessionContext.clusterTime != null) {
                expectedCommandDocument.append('$clusterTime', sessionContext.clusterTime)
            }
            expectedCommandDocument.append('lsid', sessionContext.sessionId)
        }

        if (readPreference != ReadPreference.primary()) {
            expectedCommandDocument.append('$readPreference', readPreference.toDocument())
        }
        getCommandDocument(byteBuf, messageHeader) == expectedCommandDocument

        where:
        [readPreference, sessionContext] << [
                [ReadPreference.primary(), ReadPreference.secondary()],
                [
                        Stub(SessionContext) {
                            hasSession() >> false
                        },
                        Stub(SessionContext) {
                            hasSession() >> true
                            getClusterTime() >> null
                            getSessionId() >> new BsonDocument('id', new BsonBinary([1, 2, 3] as byte[]))
                        },
                        Stub(SessionContext) {
                            hasSession() >> true
                            getClusterTime() >> new BsonDocument('clusterTime', new BsonTimestamp(42, 1))
                            getSessionId() >> new BsonDocument('id', new BsonBinary([1, 2, 3] as byte[]))
                        }
                ]
        ].combinations()
    }


    private static BsonDocument getCommandDocument(ByteBufNIO byteBuf, MessageHeader messageHeader) {
        new ReplyMessage<BsonDocument>(new ResponseBuffers(new ReplyHeader(byteBuf, messageHeader), byteBuf),
                new BsonDocumentCodec(), 0).documents.get(0)
    }
}
