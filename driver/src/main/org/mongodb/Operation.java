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

import org.mongodb.command.MongoCommand;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.io.BufferPool;
import org.mongodb.io.MongoGateway;
import org.mongodb.protocol.MongoReplyMessage;
import org.mongodb.result.CommandResult;

import java.nio.ByteBuffer;

public abstract class Operation {
    private MongoNamespace namespace;
    private final BufferPool<ByteBuffer> bufferPool;

    public Operation(final MongoNamespace namespace, final BufferPool<ByteBuffer> bufferPool) {
        this.namespace = namespace;
        this.bufferPool = bufferPool;
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }

    protected CommandResult createCommandResult(final MongoCommand commandOperation, final MongoReplyMessage<Document> replyMessage,
                                              final MongoGateway connection) {
        CommandResult commandResult = new CommandResult(commandOperation.toDocument(), connection.getAddress(),
                replyMessage.getDocuments().get(0), replyMessage.getElapsedNanoseconds());
        if (!commandResult.isOk()) {
            throw new MongoCommandFailureException(commandResult);
        }

        return commandResult;
    }

}
