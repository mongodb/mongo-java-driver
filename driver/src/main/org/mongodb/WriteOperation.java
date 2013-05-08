/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb;

import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.GetLastError;
import org.mongodb.io.BufferPool;
import org.mongodb.io.MongoGateway;
import org.mongodb.io.PooledByteBufferOutputBuffer;
import org.mongodb.io.ResponseBuffers;
import org.mongodb.operation.MongoWrite;
import org.mongodb.protocol.MongoCommandMessage;
import org.mongodb.protocol.MongoReplyMessage;
import org.mongodb.protocol.MongoRequestMessage;
import org.mongodb.result.WriteResult;

import java.nio.ByteBuffer;

public abstract class WriteOperation extends Operation {

    public WriteOperation(final MongoNamespace namespace, final BufferPool<ByteBuffer> bufferPool) {
        super(namespace, bufferPool);
    }

    public WriteResult execute(final MongoGateway connection) {
        PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferPool());
        try {
            MongoRequestMessage nextMessage = createRequestMessage().encode(buffer);
            while (nextMessage != null) {
                nextMessage = nextMessage.encode(buffer);
            }
            if (getWrite().getWriteConcern().callGetLastError()) {
                final GetLastError getLastError = new GetLastError(getWrite().getWriteConcern());
                final DocumentCodec codec = new DocumentCodec();
                MongoCommandMessage getLastErrorMessage = new MongoCommandMessage(getNamespace().getFullName(), getLastError, codec);
                getLastErrorMessage.encode(buffer);
                ResponseBuffers responseBuffers = connection.sendAndReceiveMessage(buffer);
                try {
                    return new WriteResult(getWrite(), getLastError.parseGetLastErrorResponse(
                            createCommandResult(getLastError, new MongoReplyMessage<Document>(responseBuffers, codec), connection)));
                } finally {
                    responseBuffers.close();
                }
            }
            else {
                connection.sendMessage(buffer);
                return null;
            }
        } finally {
            buffer.close();
        }
    }

    public abstract MongoWrite getWrite();

    protected abstract MongoRequestMessage createRequestMessage();

}
