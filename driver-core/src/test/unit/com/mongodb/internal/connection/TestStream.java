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

import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A test Stream implementation that handles writes (no-op) and delegates reads to subclasses.
 * Unexpected calls must be recorded via {@link #recordUnexpectedCall()} rather than JUnit's
 * {@code fail()}: the production code under test catches {@code Throwable}, so an assertion
 * error thrown inside a stub is swallowed and translated into the very exception a test expects.
 */
abstract class TestStream implements Stream {
    private int commandMessageId;
    private boolean closed;
    private final AtomicBoolean unexpectedCall = new AtomicBoolean();

    void setCommandMessageId(final int id) {
        this.commandMessageId = id;
    }

    int getCommandMessageId() {
        return commandMessageId;
    }

    boolean wasClosed() {
        return closed;
    }

    void recordUnexpectedCall() {
        unexpectedCall.set(true);
    }

    boolean hadUnexpectedCall() {
        return unexpectedCall.get();
    }

    @Override
    public void open(final OperationContext operationContext) {
    }

    @Override
    public void openAsync(final OperationContext operationContext,
            final AsyncCompletionHandler<Void> handler) {
        handler.completed(null);
    }

    @Override
    public void write(final List<ByteBuf> buffers, final OperationContext operationContext) {
    }

    @Override
    public void writeAsync(final List<ByteBuf> buffers, final OperationContext operationContext,
            final AsyncCompletionHandler<Void> handler) {
        handler.completed(null);
    }

    // Both read methods default to recording an unexpected call and throwing: a test that uses the
    // sync path overrides read() (and inherits this readAsync), and vice versa. Recording (rather
    // than calling fail()) is required because the production code catches Throwable.
    @Override
    public ByteBuf read(final int numBytes, final OperationContext operationContext) {
        recordUnexpectedCall();
        throw new UnsupportedOperationException("read should not be called in this test");
    }

    @Override
    public void readAsync(final int numBytes, final OperationContext operationContext,
            final AsyncCompletionHandler<ByteBuf> handler) {
        recordUnexpectedCall();
        throw new UnsupportedOperationException("readAsync should not be called in this test");
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return new ByteBufNIO(ByteBuffer.allocate(size));
    }

    @Override
    public ServerAddress getAddress() {
        return new ServerAddress();
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
