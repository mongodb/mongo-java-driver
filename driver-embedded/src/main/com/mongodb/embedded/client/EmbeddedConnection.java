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

import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.PointerByReference;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.List;

final class EmbeddedConnection implements Closeable {
    private volatile Pointer clientPointer;

    EmbeddedConnection(final Pointer databasePointer) {
        this.clientPointer = MongoDBCAPIHelper.db_client_new(databasePointer);
    }

    public ByteBuffer sendAndReceive(final List<ByteBuffer> messageBufferList) {
        byte[] message = createCompleteMessage(messageBufferList);

        PointerByReference outputBufferReference = new PointerByReference();
        IntByReference outputSize = new IntByReference();
        MongoDBCAPIHelper.db_client_wire_protocol_rpc(clientPointer, message, message.length, outputBufferReference, outputSize);
        return outputBufferReference.getValue().getByteBuffer(0, outputSize.getValue());
    }

    @Override
    public void close() {
        MongoDBCAPIHelper.db_client_destroy(clientPointer);
        clientPointer = null;
    }

    private byte[] createCompleteMessage(final List<ByteBuffer> buffers) {
        int totalLength = 0;
        for (ByteBuffer cur : buffers) {
            totalLength += cur.remaining();
        }
        byte[] completeMessage = new byte[totalLength];

        int offset = 0;
        for (ByteBuffer cur : buffers) {
            int remaining = cur.remaining();
            cur.get(completeMessage, offset, cur.remaining());
            offset += remaining;
        }
        return completeMessage;
    }
}
