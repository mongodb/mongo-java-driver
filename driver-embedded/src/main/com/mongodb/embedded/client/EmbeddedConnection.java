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

import com.mongodb.MongoException;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.PointerByReference;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.List;

final class EmbeddedConnection implements Closeable {

    private final MongoDBCAPI mongoDBCAPI;
    private volatile Pointer clientPointer;

    EmbeddedConnection(final MongoDBCAPI mongoDBCAPI, final Pointer databasePointer) {
        this.mongoDBCAPI = mongoDBCAPI;
        try {
            this.clientPointer = mongoDBCAPI.libmongodbcapi_db_client_new(databasePointer);
        } catch (Throwable t) {
            throw new MongoException("Error from embedded server when calling db_client_new: " + t.getMessage(), t);
        }
    }

    public ByteBuffer sendAndReceive(final List<ByteBuffer> messageBufferList) {
        byte[] message = createCompleteMessage(messageBufferList);

        PointerByReference outputBufferReference = new PointerByReference();
        IntByReference outputSize = new IntByReference();

        try {
            int errorCode = mongoDBCAPI.libmongodbcapi_db_client_wire_protocol_rpc(clientPointer, message, message.length,
                    outputBufferReference, outputSize);
            if (errorCode != 0) {
                throw new MongoException(errorCode, "Error from embedded server: " + errorCode);
            }
            return outputBufferReference.getValue().getByteBuffer(0, outputSize.getValue());
        } catch (Throwable t) {
            throw new MongoException("Error from embedded server when calling db_client_wire_protocol_rpc: " + t.getMessage(), t);
        }
    }

    @Override
    public void close() {
        try {
            mongoDBCAPI.libmongodbcapi_db_client_destroy(clientPointer);
        } catch (Throwable t) {
            throw new MongoException("Error from embedded server when calling db_client_destroy: " + t.getMessage(), t);
        }
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
