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
import com.mongodb.ServerAddress;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.io.BasicInputBuffer;
import org.bson.io.InputBuffer;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

class TestInternalConnection implements InternalConnection {
    private final ServerAddress address;
    private final BufferProvider bufferProvider;
    private final String id;
    private final Queue<ResponseBuffers> replies;
    private final List<InputBuffer> sent;

    public TestInternalConnection(final String id, final ServerAddress address) {
        this.id = id;
        this.address = address;
        this.bufferProvider = new SimpleBufferProvider();

        this.replies = new LinkedList<ResponseBuffers>();
        this.sent = new LinkedList<InputBuffer>();
    }

    public void enqueueReply(final ResponseBuffers buffers) {
        this.replies.add(buffers);
    }

    public List<InputBuffer> getSent() {
        return sent;
    }

    @Override
    public ServerAddress getServerAddress() {
        return this.address;
    }

    @Override
    public String getId() {
        return this.id;
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
        sent.add(new BasicInputBuffer(new ByteBufNIO(combined)));
    }

    @Override
    public ResponseBuffers receiveMessage() {
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
    public void receiveMessageAsync(final SingleResultCallback<ResponseBuffers> callback) {
        try {
            ResponseBuffers buffers = receiveMessage();
            callback.onResult(buffers, null);
        } catch (MongoException ex) {
            callback.onResult(null, ex);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return this.bufferProvider.getBuffer(size);
    }
}
