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
import org.bson.io.BasicInputBuffer
import org.bson.io.BasicOutputBuffer
import org.bson.io.InputBuffer
import spock.lang.Specification

class ReplyHeaderSpecification extends Specification {

    def 'should parse reply header'() {
        def outputBuffer = new BasicOutputBuffer();
        outputBuffer.with {
            writeInt(36)
            writeInt(45)
            writeInt(23)
            writeInt(1)
            writeInt(1)
            writeLong(9000)
            writeInt(4)
            writeInt(30)
        }
        InputBuffer buffer = new BasicInputBuffer(outputBuffer.byteBuffers.get(0));

        when:
        def replyHeader = new ReplyHeader(buffer);

        then:
        replyHeader.with {
            messageLength == 36
            requestId == 45
            responseTo == 23
            responseFlags == 1
            cursorId == 9000
            startingFrom == 4
            numberReturned == 30
        }
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
        InputBuffer buffer = new BasicInputBuffer(outputBuffer.byteBuffers.get(0));

        when:
        new ReplyHeader(buffer);

        then:
        thrown(MongoInternalException)
    }
}
