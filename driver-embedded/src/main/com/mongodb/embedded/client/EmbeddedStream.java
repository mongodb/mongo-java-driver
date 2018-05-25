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

package com.mongodb.embedded.client;

import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.Stream;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class EmbeddedStream implements Stream {

    private final EmbeddedConnection embeddedConnection;
    private volatile boolean isClosed;
    private volatile ByteBuffer curResponse;

    EmbeddedStream(final EmbeddedConnection embeddedConnection) {
        this.embeddedConnection = embeddedConnection;
    }

    @Override
    public void open() {
        // nothing to do here
    }

    @Override
    public void openAsync(final AsyncCompletionHandler<Void> handler) {
        // nothing to do here
        handler.completed(null);
    }

    @Override
    public void write(final List<ByteBuf> buffers) {
        curResponse = embeddedConnection.sendAndReceive(asByteBufferList(buffers));
    }

    private List<ByteBuffer> asByteBufferList(final List<ByteBuf> byteBufList) {
        List<ByteBuffer> retVal = new ArrayList<ByteBuffer>(byteBufList.size());
        for (ByteBuf cur: byteBufList) {
            retVal.add(cur.asNIO());
        }
        return retVal;
    }

    @Override
    public ByteBuf read(final int numBytes) {
        ByteBuffer slice = curResponse.slice();
        slice.limit(numBytes);
        curResponse.position(curResponse.position() + numBytes);

        return new ByteBufNIO(slice);
    }

    @Override
    public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler<Void> handler) {
       throw new UnsupportedOperationException(getClass() + " does not support asynchronous operations.");
    }

    @Override
    public void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler) {
        throw new UnsupportedOperationException(getClass() + " does not support asynchronous operations.");
    }

    @Override
    public ServerAddress getAddress() {
        return new ServerAddress();
    }

    @Override
    public void close() {
        if (!isClosed) {
            embeddedConnection.close();
            isClosed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return new ByteBufNIO(ByteBuffer.wrap(new byte[size]));
    }
}
