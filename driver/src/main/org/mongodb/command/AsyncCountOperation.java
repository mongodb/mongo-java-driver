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

package org.mongodb.command;

import org.mongodb.AsyncServerSelectingOperation;
import org.mongodb.Codec;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.Function;
import org.mongodb.MappingFuture;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ServerSelector;
import org.mongodb.operation.AsyncCommandOperation;
import org.mongodb.operation.Find;
import org.mongodb.operation.ReadPreferenceServerSelector;

public class AsyncCountOperation extends BaseCountOperation implements AsyncServerSelectingOperation<Long> {

    public AsyncCountOperation(final Find find, final MongoNamespace namespace, final Codec<Document> codec,
                               final BufferProvider bufferProvider) {
        super(find, namespace, codec, bufferProvider);
    }

    @Override
    public MongoFuture<Long> execute(final AsyncServerConnection connection) {

        MongoFuture<CommandResult> commandResultFuture = new AsyncCommandOperation(getCount().getNamespace().getDatabaseName(),
                getCount(), getCodec(), null, getBufferProvider()).execute(connection);
        return new MappingFuture<CommandResult, Long>(commandResultFuture, new Function<CommandResult, Long>() {
            @Override
            public Long apply(final CommandResult commandResult) {
                return getCount(commandResult);
            }
        });
    }

    @Override
    public ServerSelector getServerSelector() {
        return new ReadPreferenceServerSelector(getCount().getReadPreference());
    }

    @Override
    public boolean isQuery() {
        return true;
    }
}
