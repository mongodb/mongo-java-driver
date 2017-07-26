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

package com.mongodb.connection;

import org.bson.ByteBuf;
import org.bson.io.BsonOutput;

import java.nio.ByteOrder;
import java.util.List;

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
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput, final int messageStartPosition) {
        int wrappedUncompressedSize = 0;
        for (ByteBuf cur : wrappedMessageBuffers) {
            wrappedUncompressedSize += cur.limit() - cur.position();  // TODO: necessary to subtract position?
        }

        bsonOutput.writeInt32(wrappedOpcode.getValue());
        bsonOutput.writeInt32(wrappedUncompressedSize - 16);
        bsonOutput.writeByte(compressor.getId());

        compressor.compress(wrappedMessageBuffers, bsonOutput);

        return new EncodingMetadata(null, 0);
    }

    private static int getWrappedMessageRequestId(final List<ByteBuf> wrappedMessageBuffers) {
        // TODO: ugh... the byte order isn't correct for reading
        return wrappedMessageBuffers.get(0).order(ByteOrder.LITTLE_ENDIAN).getInt(4);
    }
}
