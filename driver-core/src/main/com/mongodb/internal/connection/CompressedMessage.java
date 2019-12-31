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

package com.mongodb.internal.connection;

import com.mongodb.internal.session.SessionContext;
import org.bson.ByteBuf;
import org.bson.io.BsonOutput;

import java.util.List;

import static com.mongodb.internal.connection.MessageHeader.MESSAGE_HEADER_LENGTH;

class CompressedMessage extends RequestMessage {
    private final OpCode wrappedOpcode;
    private final List<ByteBuf> wrappedMessageBuffers;
    private final Compressor compressor;

    CompressedMessage(final OpCode wrappedOpcode, final List<ByteBuf> wrappedMessageBuffers, final Compressor compressor,
                      final MessageSettings settings) {
        super(OpCode.OP_COMPRESSED, getWrappedMessageRequestId(wrappedMessageBuffers), settings);
        this.wrappedOpcode = wrappedOpcode;
        this.wrappedMessageBuffers = wrappedMessageBuffers;
        this.compressor = compressor;
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput, final SessionContext sessionContext) {
        bsonOutput.writeInt32(wrappedOpcode.getValue());
        bsonOutput.writeInt32(getWrappedMessageSize(wrappedMessageBuffers) - MESSAGE_HEADER_LENGTH);
        bsonOutput.writeByte(compressor.getId());

        getFirstWrappedMessageBuffer(wrappedMessageBuffers)
                .position(getFirstWrappedMessageBuffer(wrappedMessageBuffers).position() + MESSAGE_HEADER_LENGTH);

        compressor.compress(wrappedMessageBuffers, bsonOutput);

        return new EncodingMetadata(0);
    }

    private static int getWrappedMessageSize(final List<ByteBuf> wrappedMessageBuffers) {
        ByteBuf first = getFirstWrappedMessageBuffer(wrappedMessageBuffers);
        return first.getInt(0);
    }

    private static int getWrappedMessageRequestId(final List<ByteBuf> wrappedMessageBuffers) {
        ByteBuf first = getFirstWrappedMessageBuffer(wrappedMessageBuffers);
        return first.getInt(4);
    }

    private static ByteBuf getFirstWrappedMessageBuffer(final List<ByteBuf> wrappedMessageBuffers) {
        return wrappedMessageBuffers.get(0);
    }
}
