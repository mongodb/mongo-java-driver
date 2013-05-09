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

import org.mongodb.CommandOperation;
import org.mongodb.Document;
import org.mongodb.MongoServerBinding;
import org.mongodb.ServerAddress;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.IsMasterCommandResult;
import org.mongodb.command.MongoCommand;

class MongoConnectionIsMasterExecutor implements IsMasterExecutor {
    private final MongoServerBinding binding;
    private final ServerAddress serverAddress;

    MongoConnectionIsMasterExecutor(final MongoServerBinding binding, final ServerAddress serverAddress) {
        this.binding = binding;
        this.serverAddress = serverAddress;
    }

    @Override
    public IsMasterCommandResult execute() {
        return new IsMasterCommandResult(
                new CommandOperation("admin", new MongoCommand(new Document("ismaster", 1)), new DocumentCodec(), binding.getBufferPool())
                .execute(binding));
    }

    @Override
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    @Override
    public void close() {
        binding.close();
    }
}
