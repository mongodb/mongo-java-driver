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

import org.bson.BsonDocument;
import org.bson.ByteBuf;
import org.bson.codecs.Decoder;

import java.io.Closeable;

public class ResponseBuffers implements Closeable {
    private final ReplyHeader replyHeader;
    private final ByteBuf bodyByteBuffer;
    private final int bodyByteBufferStartPosition;
    private volatile boolean isClosed;

    ResponseBuffers(final ReplyHeader replyHeader, final ByteBuf bodyByteBuffer) {
        this.replyHeader = replyHeader;
        this.bodyByteBuffer = bodyByteBuffer;
        this.bodyByteBufferStartPosition = bodyByteBuffer == null ? 0 : bodyByteBuffer.position();
    }

    /**
     * Gets the reply header.
     *
     * @return the reply header
     */
    public ReplyHeader getReplyHeader() {
        return replyHeader;
    }

    <T extends BsonDocument> T getResponseDocument(final int messageId, final Decoder<T> decoder) {
        ReplyMessage<T> replyMessage = new ReplyMessage<T>(this, decoder, messageId);
        reset();
        return replyMessage.getDocuments().get(0);
    }

    /**
     * Returns a read-only buffer containing the response body.  Care should be taken to not use the returned buffer after this instance has
     * been closed.
     *
     * @return a read-only buffer containing the response body
     */
    public ByteBuf getBodyByteBuffer() {
        return bodyByteBuffer.asReadOnly();
    }

    public void reset() {
        bodyByteBuffer.position(bodyByteBufferStartPosition);
    }

    @Override
    public void close() {
        if (!isClosed) {
            if (bodyByteBuffer != null) {
                bodyByteBuffer.release();
            }
            isClosed = true;
        }
    }
}
