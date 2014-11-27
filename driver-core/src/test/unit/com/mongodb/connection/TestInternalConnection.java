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

import com.mongodb.MongoException;
import com.mongodb.async.SingleResultCallback;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.io.BsonInput;
import org.bson.io.ByteBufferBsonInput;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

class TestInternalConnection implements InternalConnection {
    private final ConnectionDescription description;
    private final BufferProvider bufferProvider;
    private final Deque<ResponseBuffers> replies;
    private final List<BsonInput> sent;
    private boolean opened;
    private boolean closed;

    public TestInternalConnection(final ServerId serverId) {
        this.description = new ConnectionDescription(serverId);
        this.bufferProvider = new SimpleBufferProvider();

        this.replies = new LinkedList<ResponseBuffers>();
        this.sent = new LinkedList<BsonInput>();
    }

    public void enqueueReply(final ResponseBuffers buffers) {
        this.replies.add(buffers);
    }

    public List<BsonInput> getSent() {
        return sent;
    }

    @Override
    public ConnectionDescription getDescription() {
        return description;
    }

    public void open() {
        opened = true;
    }

    @Override
    public void openAsync(final SingleResultCallback<Void> callback) {
        opened = true;
        callback.onResult(null, null);
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean opened() {
        return opened;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        // repackage all byte buffers into a single byte buffer...
        int totalSize = 0;
        for (ByteBuf buf : byteBuffers) {
            totalSize += buf.remaining();
        }

        ByteBuffer combined = ByteBuffer.allocate(totalSize);
        for (ByteBuf buf : byteBuffers) {
            combined.put(buf.array(), 0, buf.remaining());
        }

        combined.flip();

        ResponseBuffers nextToReceive = replies.removeFirst();
        ReplyHeader header = replaceResponseTo(nextToReceive.getReplyHeader(), lastRequestId);
        replies.addFirst(new ResponseBuffers(header, nextToReceive.getBodyByteBuffer()));

        sent.add(new ByteBufferBsonInput(new ByteBufNIO(combined)));
    }

    private ReplyHeader replaceResponseTo(final ReplyHeader header, final int responseTo) {
        ByteBuffer headerByteBuffer = ByteBuffer.allocate(36);
        headerByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        headerByteBuffer.putInt(header.getMessageLength());
        headerByteBuffer.putInt(header.getRequestId());
        headerByteBuffer.putInt(responseTo);
        headerByteBuffer.putInt(1);
        headerByteBuffer.putInt(header.getResponseFlags());
        headerByteBuffer.putLong(header.getCursorId());
        headerByteBuffer.putInt(header.getStartingFrom());
        headerByteBuffer.putInt(header.getNumberReturned());
        headerByteBuffer.flip();

        ByteBufferBsonInput headerInputBuffer = new ByteBufferBsonInput(new ByteBufNIO(headerByteBuffer));
        return new ReplyHeader(headerInputBuffer);
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        if (this.replies.isEmpty()) {
            throw new MongoException("Test was not setup properly as too many calls to receiveMessage occured.");
        }

        return this.replies.remove();
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        sendMessage(byteBuffers, lastRequestId);
        callback.onResult(null, null);
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        try {
            ResponseBuffers buffers = receiveMessage(responseTo);
            callback.onResult(buffers, null);
        } catch (MongoException ex) {
            callback.onResult(null, ex);
        }
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return this.bufferProvider.getBuffer(size);
    }
}
