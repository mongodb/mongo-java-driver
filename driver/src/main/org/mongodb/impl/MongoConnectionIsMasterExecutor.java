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

package org.mongodb.impl;

import org.mongodb.Document;
import org.mongodb.MongoConnector;
import org.mongodb.ServerAddress;
import org.mongodb.command.IsMasterCommandResult;
import org.mongodb.operation.MongoCommand;
import org.mongodb.PrimitiveCodecs;
import org.mongodb.codecs.DocumentCodec;

class MongoConnectionIsMasterExecutor implements IsMasterExecutor {
    private final MongoConnector connector;
    private final ServerAddress serverAddress;

    MongoConnectionIsMasterExecutor(final MongoConnector connector, final ServerAddress serverAddress) {
        this.connector = connector;
        this.serverAddress = serverAddress;
    }

    @Override
    public IsMasterCommandResult execute() {
        return new IsMasterCommandResult(
                connector.command("admin", new MongoCommand(new Document("ismaster", 1)),
                        new DocumentCodec(PrimitiveCodecs.createDefault())));
    }

    @Override
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    @Override
    public void close() {
        connector.close();
    }
}
