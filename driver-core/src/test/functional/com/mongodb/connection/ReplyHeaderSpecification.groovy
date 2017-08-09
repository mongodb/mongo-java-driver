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

import com.mongodb.MongoInternalException
import org.bson.io.BasicOutputBuffer
import spock.lang.Specification

import static com.mongodb.connection.ConnectionDescription.getDefaultMaxMessageSize

class ReplyHeaderSpecification extends Specification {

    def 'should parse reply header'() {
        def outputBuffer = new BasicOutputBuffer();
        outputBuffer.with {
            writeInt(186)
            writeInt(45)
            writeInt(23)
            writeInt(1)
            writeInt(responseFlags)
            writeLong(9000)
            writeInt(4)
            writeInt(30)
        }
        def byteBuf = outputBuffer.byteBuffers.get(0)

        when:
        def replyHeader = new ReplyHeader(byteBuf, new MessageHeader(byteBuf, getDefaultMaxMessageSize()));

        then:
        replyHeader.messageLength == 186
        replyHeader.requestId == 45
        replyHeader.responseTo == 23
        replyHeader.responseFlags == responseFlags
        replyHeader.cursorId == 9000
        replyHeader.startingFrom == 4
        replyHeader.numberReturned == 30
        replyHeader.cursorNotFound == cursorNotFound
        replyHeader.queryFailure == queryFailure

        where:
        responseFlags << [0, 1, 2, 3]
        cursorNotFound << [false, true, false, true]
        queryFailure << [false, false, true, true]
    }

    def 'should parse reply header with compressed header'() {

        def outputBuffer = new BasicOutputBuffer();
        outputBuffer.with {
            writeInt(186)
            writeInt(45)
            writeInt(23)
            writeInt(2012)
            writeInt(1)
            writeInt(258)
            writeByte(2)
            writeInt(responseFlags)
            writeLong(9000)
            writeInt(4)
            writeInt(30)
        }
        def byteBuf = outputBuffer.byteBuffers.get(0)
        def compressedHeader = new CompressedHeader(byteBuf, new MessageHeader(byteBuf, getDefaultMaxMessageSize()))

        when:
        def replyHeader = new ReplyHeader(byteBuf, compressedHeader);

        then:
        replyHeader.messageLength == 274
        replyHeader.requestId == 45
        replyHeader.responseTo == 23
        replyHeader.responseFlags == responseFlags
        replyHeader.cursorId == 9000
        replyHeader.startingFrom == 4
        replyHeader.numberReturned == 30
        replyHeader.cursorNotFound == cursorNotFound
        replyHeader.queryFailure == queryFailure

        where:
        responseFlags << [0, 1, 2, 3]
        cursorNotFound << [false, true, false, true]
        queryFailure << [false, false, true, true]
    }

    def 'should throw MongoInternalException on incorrect opCode'() {
        def outputBuffer = new BasicOutputBuffer();
        outputBuffer.with {
            writeInt(36)
            writeInt(45)
            writeInt(23)
            writeInt(2)
            writeInt(0)
            writeLong(2)
            writeInt(0)
            writeInt(0)
        }
        def byteBuf = outputBuffer.byteBuffers.get(0)

        when:
        new ReplyHeader(byteBuf, new MessageHeader(byteBuf, getDefaultMaxMessageSize()));

        then:
        def ex = thrown(MongoInternalException)
        ex.getMessage() == 'Unexpected reply message opCode 2'
    }

    def 'should throw MongoInternalException on message size < 36'() {
        def outputBuffer = new BasicOutputBuffer();
        outputBuffer.with {
            writeInt(35)
            writeInt(45)
            writeInt(23)
            writeInt(1)
            writeInt(0)
            writeLong(2)
            writeInt(0)
            writeInt(0)
        }
        def byteBuf = outputBuffer.byteBuffers.get(0)

        when:
        new ReplyHeader(byteBuf, new MessageHeader(byteBuf, getDefaultMaxMessageSize()));

        then:
        def ex = thrown(MongoInternalException)
        ex.getMessage() == 'The reply message length 35 is less than the mimimum message length 36'
    }

    def 'should throw MongoInternalException on message size > max message size'() {
        def outputBuffer = new BasicOutputBuffer();
        outputBuffer.with {
            writeInt(400)
            writeInt(45)
            writeInt(23)
            writeInt(1)
            writeInt(0)
            writeLong(2)
            writeInt(0)
            writeInt(0)
        }
        def byteBuf = outputBuffer.byteBuffers.get(0)

        when:
        new ReplyHeader(byteBuf, new MessageHeader(byteBuf, 399));

        then:
        def ex = thrown(MongoInternalException)
        ex.getMessage() == 'The reply message length 400 is less than the maximum message length 399'
    }

    def 'should throw MongoInternalException on num documents < 0'() {
        def outputBuffer = new BasicOutputBuffer();
        outputBuffer.with {
            writeInt(186)
            writeInt(45)
            writeInt(23)
            writeInt(1)
            writeInt(1)
            writeLong(9000)
            writeInt(4)
            writeInt(-1)
        }
        def byteBuf = outputBuffer.byteBuffers.get(0)

        when:
        new ReplyHeader(byteBuf, new MessageHeader(byteBuf, getDefaultMaxMessageSize()));

        then:
        def ex = thrown(MongoInternalException)
        ex.getMessage() == 'The reply message number of returned documents, -1, is less than 0'
    }

    def 'should throw MongoInternalException on num documents < 0 with compressed header'() {

        def outputBuffer = new BasicOutputBuffer();
        outputBuffer.with {
            writeInt(186)
            writeInt(45)
            writeInt(23)
            writeInt(2012)
            writeInt(1)
            writeInt(258)
            writeByte(2)
            writeInt(1)
            writeLong(9000)
            writeInt(4)
            writeInt(-1)
        }
        def byteBuf = outputBuffer.byteBuffers.get(0)
        def compressedHeader = new CompressedHeader(byteBuf, new MessageHeader(byteBuf, getDefaultMaxMessageSize()))

        when:
        new ReplyHeader(byteBuf, compressedHeader);

        then:
        def ex = thrown(MongoInternalException)
        ex.getMessage() == 'The reply message number of returned documents, -1, is less than 0'
    }
}