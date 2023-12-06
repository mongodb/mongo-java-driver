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

import com.mongodb.MongoInternalException;
import org.bson.BsonBinaryReader;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.io.BsonInput;
import org.bson.io.ByteBufferBsonInput;

import static java.lang.String.format;

/**
 * An OP_REPLY message.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ReplyMessage<T> {

    private final T document;

    public ReplyMessage(final ResponseBuffers responseBuffers, final Decoder<T> decoder, final long requestId) {
        if (requestId != responseBuffers.getReplyHeader().getResponseTo()) {
            throw new MongoInternalException(format("The responseTo (%d) in the response does not match the requestId (%d) in the "
                    + "request", responseBuffers.getReplyHeader().getResponseTo(), requestId));
        }

        try (BsonInput bsonInput = new ByteBufferBsonInput(responseBuffers.getBodyByteBuffer().duplicate())) {
            try (BsonBinaryReader reader = new BsonBinaryReader(bsonInput)) {
                document = decoder.decode(reader, DecoderContext.builder().build());
            }
        } finally {
            responseBuffers.reset();
        }
    }

    public T getDocument() {
        return document;
    }
}
