/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.operation;

import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ReadBinding;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.mongodb.CommandResult;
import org.mongodb.Function;

import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;

/**
 * Return the ping time from mongo
 *
 * @mongodb.server.release 2.6
 * @since 3.0
 */
public class PingOperation implements AsyncReadOperation<Double>, ReadOperation<Double> {
    private static final String ADMIN_DATABASE = "admin";
    private static final BsonDocument PING_COMMAND = new BsonDocument("ping", new BsonInt32(1));

    @Override
    public MongoFuture<Double> executeAsync(final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(ADMIN_DATABASE, PING_COMMAND, binding, transformer());
    }

    @Override
    public Double execute(final ReadBinding binding) {
        return executeWrappedCommandProtocol(ADMIN_DATABASE, PING_COMMAND, binding, transformer());
    }

    //TODO: it's not clear from the documentation what the return type should be
    //http://docs.mongodb.org/manual/reference/command/ping/
    @SuppressWarnings("unchecked")
    private Function<CommandResult, Double> transformer() {
        return new Function<CommandResult, Double>() {
            @Override
            public Double apply(final CommandResult result) {
                return result.getResponse().getDouble("ok").doubleValue();
            }
        };
    }
}
