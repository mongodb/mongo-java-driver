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

package org.mongodb.connection;

import org.mongodb.Document;
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.MongoCommand;
import org.mongodb.operation.CommandOperation;
import org.mongodb.operation.CommandResult;

import java.nio.ByteBuffer;

@ThreadSafe
class IsMasterServerStateNotifier implements ServerStateNotifier {

    private final ServerStateListener serverStateListener;
    private final ConnectionFactory connectionFactory;
    private final BufferPool<ByteBuffer> bufferPool;
    private Connection connection;
    private int count;
    private long elapsedNanosSum;

    IsMasterServerStateNotifier(final ServerStateListener serverStateListener, final ConnectionFactory connectionFactory,
                                final BufferPool<ByteBuffer> bufferPool) {
        this.serverStateListener = serverStateListener;
        this.connectionFactory = connectionFactory;
        this.bufferPool = bufferPool;
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized void run() {
        try {
            if (connection == null) {
                connection = connectionFactory.create();
            }
            try {
                final CommandResult commandResult = new CommandOperation("admin",
                        new MongoCommand(new Document("ismaster", 1)), new DocumentCodec(), bufferPool).execute(connection);
                count++;
                elapsedNanosSum += commandResult.getElapsedNanoseconds();

                ServerDescription serverDescription = new ServerDescription(commandResult, elapsedNanosSum / count);
                serverStateListener.notify(serverDescription);
            } catch (MongoSocketException e) {
                connection.close();
                connection = null;
                count = 0;
                elapsedNanosSum = 0;
                throw e;
            }
        } catch (MongoException e) {
            serverStateListener.notify(e);
        } catch (Throwable t) {
            serverStateListener.notify(new MongoInternalException("Unexpected exception", t));
        }
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }
}
