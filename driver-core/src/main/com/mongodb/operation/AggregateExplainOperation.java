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

import com.mongodb.MongoNamespace;
import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ReadBinding;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.mongodb.CommandResult;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;

/**
 * An operation that executes an explain on an aggregation pipeline.
 *
 * @since 3.0
 */
public class AggregateExplainOperation implements AsyncReadOperation<CommandResult>, ReadOperation<CommandResult> {
    private final MongoNamespace namespace;
    private final List<BsonDocument> pipeline;
    private final AggregationOptions options;

    /**
     * Constructs a new instance.
     *
     * @param namespace the namespace
     * @param pipeline  the aggregation pipeline
     * @param options   the aggregation options
     */
    public AggregateExplainOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline, final AggregationOptions options) {
        this.namespace = notNull("namespace", namespace);
        this.pipeline = notNull("pipeline", pipeline);
        this.options = notNull("options", options);
    }

    @Override
    public CommandResult execute(final ReadBinding binding) {
        return executeWrappedCommandProtocol(namespace, getCommand(), binding);
    }

    @Override
    public MongoFuture<CommandResult> executeAsync(final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace, getCommand(), binding);
    }

    private BsonDocument getCommand() {
        BsonDocument command = AggregateHelper.asCommandDocument(namespace, pipeline, options);
        command.put("explain", BsonBoolean.TRUE);
        return command;
    }

}
