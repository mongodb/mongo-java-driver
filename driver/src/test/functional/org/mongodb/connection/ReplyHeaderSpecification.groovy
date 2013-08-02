/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection

import org.bson.io.BasicInputBuffer
import org.bson.io.BasicOutputBuffer
import org.bson.io.InputBuffer
import org.mongodb.FunctionalSpecification
import org.mongodb.MongoInternalException

class ReplyHeaderSpecification extends FunctionalSpecification {

    def 'should parse reply header'() {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        outputBuffer.writeInt(36);
        outputBuffer.writeInt(45);
        outputBuffer.writeInt(23);
        outputBuffer.writeInt(1);
        outputBuffer.writeInt(1);
        outputBuffer.writeLong(9000);
        outputBuffer.writeInt(4);
        outputBuffer.writeInt(30);
        InputBuffer buffer = new BasicInputBuffer(outputBuffer.byteBuffers.get(0));

        when:
        def replyHeader = new ReplyHeader(buffer);

        then:
        replyHeader.messageLength == 36
        replyHeader.requestId == 45
        replyHeader.responseTo == 23
        replyHeader.responseFlags == 1
        replyHeader.cursorId == 9000
        replyHeader.startingFrom == 4
        replyHeader.numberReturned == 30
    }

    def 'should throw MongoInternalException on incorrect opCode'() {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        outputBuffer.writeInt(36);
        outputBuffer.writeInt(45);
        outputBuffer.writeInt(23);
        outputBuffer.writeInt(2);
        outputBuffer.writeInt(0);
        outputBuffer.writeLong(2);
        outputBuffer.writeInt(0);
        outputBuffer.writeInt(0);
        InputBuffer buffer = new BasicInputBuffer(outputBuffer.byteBuffers.get(0));

        when:
        new ReplyHeader(buffer);

        then:
        thrown(MongoInternalException)
    }
}
