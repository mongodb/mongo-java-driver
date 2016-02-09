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

import org.bson.ByteBuf;

import java.io.Closeable;

class ResponseBuffers implements Closeable {
    private final ReplyHeader replyHeader;
    private final ByteBuf bodyByteBuffer;
    private volatile boolean isClosed;

    /**
     * Construct an instance.
     *
     * @param replyHeader the reply header
     * @param bodyByteBuffer a byte buffer containing the message body
     */
    public ResponseBuffers(final ReplyHeader replyHeader, final ByteBuf bodyByteBuffer) {
        this.replyHeader = replyHeader;
        this.bodyByteBuffer = bodyByteBuffer;
    }

    /**
     * Gets the reply header.
     *
     * @return the reply header
     */
    public ReplyHeader getReplyHeader() {
        return replyHeader;
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
        bodyByteBuffer.position(0);
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
