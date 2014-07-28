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

package org.mongodb.operation;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;
import org.mongodb.AggregationOptions;
import org.mongodb.CommandResult;
import org.mongodb.Function;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.binding.AsyncReadBinding;
import org.mongodb.binding.ReadBinding;

import java.util.List;

import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;

/**
 * An operation that executes an explain on an aggregation pipeline.
 *
 * @since 3.0
 */
public class AggregateExplainOperation<T> implements AsyncReadOperation<CommandResult<T>>, ReadOperation<CommandResult<T>> {
    private final MongoNamespace namespace;
    private final List<BsonDocument> pipeline;
    private final AggregationOptions options;
    private final Decoder<T> decoder;

    /**
     * Constructs a new instance.
     *
     * @param namespace the namespace
     * @param pipeline  the aggregation pipeline
     * @param decoder   the decoder
     * @param options   the aggregation options
     */
    public AggregateExplainOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline, final AggregationOptions options,
                                     final Decoder<T> decoder) {
        this.namespace = namespace;
        this.pipeline = pipeline;
        this.options = options;
        this.decoder = decoder;
    }

    @Override
    public CommandResult<T> execute(final ReadBinding binding) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(), binding, transformer());
    }

    @Override
    public MongoFuture<CommandResult<T>> executeAsync(final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), getCommand(), binding, transformer());
    }

    private BsonDocument getCommand() {
        BsonDocument command = AggregateHelper.asCommandDocument(namespace, pipeline, options);
        command.put("explain", BsonBoolean.TRUE);
        return command;
    }

    private Function<CommandResult<BsonDocument>, CommandResult<T>> transformer() {
        return new Function<CommandResult<BsonDocument>, CommandResult<T>>() {

            @Override
            public CommandResult<T> apply(final CommandResult<BsonDocument> commandResult) {
                return new CommandResult<T>(commandResult.getAddress(), commandResult.getResponse(),
                                            commandResult.getElapsedNanoseconds(), decoder);
            }
        };
    }

}
