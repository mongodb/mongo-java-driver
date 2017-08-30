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

import com.mongodb.MongoNamespace
import com.mongodb.WriteConcern
import com.mongodb.bulk.InsertRequest
import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.ByteBufNIO
import org.bson.codecs.BsonDocumentCodec
import org.bson.io.BasicOutputBuffer
import spock.lang.Specification

import java.nio.ByteBuffer

class WriteCommandMessageSpecification extends Specification {
    def 'should encode write command message with OP_MSG'() {
        given:
        def message = new InsertCommandMessage(new MongoNamespace('db.test'), ordered, writeConcern, bypassDocumentValidation,
                MessageSettings.builder()
                        .serverVersion(new ServerVersion(3, 6))
                        .build(),
                [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))])
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
                .append('insert', new BsonString('test'))
                .append('ordered', new BsonBoolean(ordered))

        if (!writeConcern.isServerDefault()) {
            expectedCommandDocument.append('writeConcern', writeConcern.asDocument())
        }
        if (bypassDocumentValidation != null) {
            expectedCommandDocument.append('bypassDocumentValidation', new BsonBoolean(bypassDocumentValidation))
        }
        expectedCommandDocument
                .append('documents', new BsonArray([new BsonDocument('_id', new BsonInt32(1))]))
                .append('$db', new BsonString('db'))

        if (sessionContext.hasSession()) {
            if (sessionContext.clusterTime != null) {
                expectedCommandDocument.append('$clusterTime', sessionContext.clusterTime)
            }
            expectedCommandDocument.append('lsid', sessionContext.sessionId)
        }

        getCommandDocument(byteBuf, messageHeader) == expectedCommandDocument

        where:
        [ordered, writeConcern, bypassDocumentValidation, sessionContext] << [
                [true, false],
                [WriteConcern.ACKNOWLEDGED, WriteConcern.MAJORITY],
                [true, false, null],
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
