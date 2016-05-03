/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.async.client.gridfs

import com.mongodb.async.FutureResultCallback

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousByteChannel
import java.nio.channels.CompletionHandler
import java.util.concurrent.Future

import static java.util.concurrent.TimeUnit.SECONDS

class GridFSTestHelper {

    static run(operation, ... args) {
        runOp(operation, 60, *args)
    }

    static runSlow(operation, ... args) {
        runOp(operation, 180, *args)
    }

    static runOp(operation, timeout, ... args) {
        FutureResultCallback futureResultCallback = new FutureResultCallback()
        List opArgs = (args != null) ? args : []
        operation.call(*opArgs + futureResultCallback)
        futureResultCallback.get(timeout, SECONDS)
    }

    static class TestAsynchronousByteChannel implements AsynchronousByteChannel, Closeable {
        private boolean closed
        private final readBuffer
        private final writeBuffer = ByteBuffer.allocate(1024)

        TestAsynchronousByteChannel(final readBuffer) {
            this.readBuffer = readBuffer
        }

        ByteBuffer getReadBuffer() {
            readBuffer.flip()
        }

        ByteBuffer getWriteBuffer() {
            writeBuffer.flip()
        }

        @Override
        <A> void read(final ByteBuffer dst, final A attachment, final CompletionHandler<Integer, ? super A> handler) {
            int transferAmount = Math.min(dst.remaining(), readBuffer.remaining());
            if (transferAmount == 0 ) {
                transferAmount = -1
            }
            if (transferAmount > 0) {
                ByteBuffer temp = readBuffer.duplicate()
                temp.limit(temp.position() + transferAmount)
                dst.put(temp)
                readBuffer.position(readBuffer.position() + transferAmount)
            }
            handler.completed(transferAmount, attachment)
        }

        @Override
        Future<Integer> read(final ByteBuffer dst) {
            throw new UnsupportedOperationException('Not Supported')
        }

        @Override
        <A> void write(final ByteBuffer src, final A attachment, final CompletionHandler<Integer, ? super A> handler) {
            int transferAmount = Math.min(src.remaining(), writeBuffer.remaining());
            if (transferAmount == 0 ) {
                transferAmount = -1
            }
            if (transferAmount > 0) {
                ByteBuffer temp = src.duplicate()
                temp.limit(temp.position() + transferAmount)
                writeBuffer.put(temp)
            }
            handler.completed(transferAmount, attachment)
        }

        @Override
        Future<Integer> write(final ByteBuffer src) {
            throw new UnsupportedOperationException('Not Supported')
        }

        @Override
        void close() throws IOException {
            closed = true
        }

        @Override
        boolean isOpen() {
            !closed;
        }
    }
}
