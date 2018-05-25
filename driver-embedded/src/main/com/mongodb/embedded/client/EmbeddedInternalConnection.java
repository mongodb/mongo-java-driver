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

import com.mongodb.MongoCompressor;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.Stream;
import com.mongodb.connection.StreamFactory;
import com.mongodb.event.CommandListener;
import com.mongodb.internal.connection.Authenticator;
import com.mongodb.internal.connection.CommandMessage;
import com.mongodb.internal.connection.InternalConnection;
import com.mongodb.internal.connection.InternalStreamConnection;
import com.mongodb.internal.connection.InternalStreamConnectionInitializer;
import com.mongodb.internal.connection.ResponseBuffers;
import com.mongodb.session.SessionContext;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.PointerByReference;
import org.bson.BsonDocument;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.codecs.Decoder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class EmbeddedInternalConnection implements InternalConnection {
    private final InternalConnection wrapped;
    private volatile Pointer clientPointer;

    EmbeddedInternalConnection(final Pointer databasePointer, final CommandListener commandListener,
                               final BsonDocument clientMetadataDocument) {
        this.clientPointer = MongoDBCAPIHelper.db_client_new(databasePointer);
        this.wrapped = new InternalStreamConnection(new ServerId(new ClusterId(), new ServerAddress()),
                new StreamFactory() {
                    @Override
                    public Stream create(final ServerAddress serverAddress) {
                        return new EmbeddedInternalStream();
                    }
                }, Collections.<MongoCompressor>emptyList(), commandListener,
                new InternalStreamConnectionInitializer(Collections.<Authenticator>emptyList(), clientMetadataDocument,
                        Collections.<MongoCompressor>emptyList()));
    }

    @Override
    public ConnectionDescription getDescription() {
        return wrapped.getDescription();
    }

    @Override
    public void open() {
        wrapped.open();
    }

    @Override
    public void openAsync(final SingleResultCallback<Void> callback) {
        wrapped.openAsync(callback);
    }

    @Override
    public void close() {
        if (!wrapped.isClosed()) {
            wrapped.close();
            MongoDBCAPIHelper.db_client_destroy(clientPointer);
            clientPointer = null;
        }
    }

    @Override
    public boolean opened() {
        return wrapped.opened();
    }

    @Override
    public boolean isClosed() {
        return wrapped.isClosed();
    }

    @Override
    public <T> T sendAndReceive(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
        return wrapped.sendAndReceive(message, decoder, sessionContext);
    }

    @Override
    public <T> void sendAndReceiveAsync(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext,
                                        final SingleResultCallback<T> callback) {
        wrapped.sendAndReceiveAsync(message, decoder, sessionContext, callback);
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        wrapped.sendMessage(byteBuffers, lastRequestId);
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        return wrapped.receiveMessage(responseTo);
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        wrapped.sendMessageAsync(byteBuffers, lastRequestId, callback);
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        wrapped.receiveMessageAsync(responseTo, callback);
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return wrapped.getBuffer(size);
    }

   class EmbeddedInternalStream implements Stream {
        private volatile boolean isClosed;
        private volatile ByteBuffer curResponse;

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
            byte[] message = createCompleteMessage(buffers);

            PointerByReference outputBufferReference = new PointerByReference();
            IntByReference outputSize = new IntByReference();
            MongoDBCAPIHelper.db_client_wire_protocol_rpc(clientPointer, message, message.length, outputBufferReference, outputSize);
            curResponse = outputBufferReference.getValue().getByteBuffer(0, outputSize.getValue());
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
            return wrapped.getDescription().getServerAddress();
        }

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public boolean isClosed() {
            return isClosed;
        }

        @Override
        public ByteBuf getBuffer(final int size) {
            return new ByteBufNIO(ByteBuffer.wrap(new byte[size]));
        }

        private byte[] createCompleteMessage(final List<ByteBuf> byteBufList) {
            List<ByteBuffer> buffers = asByteBufferList(byteBufList);
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

        private List<ByteBuffer> asByteBufferList(final List<ByteBuf> byteBufList) {
            List<ByteBuffer> retVal = new ArrayList<ByteBuffer>(byteBufList.size());
            for (ByteBuf cur: byteBufList) {
                retVal.add(cur.asNIO());
            }
            return retVal;
        }
    }
}
