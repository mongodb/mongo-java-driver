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

package com.mongodb.connection;

import com.mongodb.MongoInternalException;
import org.bson.ByteBufNIO;
import org.bson.Document;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.mongodb.connection.ConnectionDescription.getDefaultMaxMessageSize;

public class ReplyMessageTest {
    @Test(expected = MongoInternalException.class)
    public void shouldThrowExceptionIfRequestIdDoesNotMatchResponseTo() {
        int badResponseTo = 34565;
        int expectedResponseTo = 5;

        ByteBuffer headerByteBuffer = ByteBuffer.allocate(36);
        headerByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        headerByteBuffer.putInt(36);
        headerByteBuffer.putInt(2456);
        headerByteBuffer.putInt(badResponseTo);
        headerByteBuffer.putInt(1);
        headerByteBuffer.putInt(0);
        headerByteBuffer.putLong(0);
        headerByteBuffer.putInt(0);
        headerByteBuffer.putInt(0);
        headerByteBuffer.flip();

        ByteBufNIO byteBuf = new ByteBufNIO(headerByteBuffer);
        ReplyHeader replyHeader = new ReplyHeader(byteBuf, new MessageHeader(byteBuf, getDefaultMaxMessageSize()));
        new ReplyMessage<Document>(replyHeader, expectedResponseTo);
    }

    @Test(expected = MongoInternalException.class)
    public void shouldThrowExceptionIfOpCodeIsIncorrect() {
        int badOpCode = 2;

        ByteBuffer headerByteBuffer = ByteBuffer.allocate(36);
        headerByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        headerByteBuffer.putInt(36);
        headerByteBuffer.putInt(2456);
        headerByteBuffer.putInt(5);
        headerByteBuffer.putInt(badOpCode);
        headerByteBuffer.putInt(0);
        headerByteBuffer.putLong(0);
        headerByteBuffer.putInt(0);
        headerByteBuffer.putInt(0);
        headerByteBuffer.flip();

        ByteBufNIO byteBuf = new ByteBufNIO(headerByteBuffer);
        ReplyHeader replyHeader = new ReplyHeader(byteBuf, new MessageHeader(byteBuf, getDefaultMaxMessageSize()));
        new ReplyMessage<Document>(replyHeader, 5);
    }
}
