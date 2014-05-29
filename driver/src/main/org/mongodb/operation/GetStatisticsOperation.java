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

import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.Function;
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.binding.AsyncReadBinding;
import org.mongodb.binding.ReadBinding;

import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static org.mongodb.operation.CommandOperationHelper.ignoreNameSpaceErrors;
import static org.mongodb.operation.OperationHelper.transformResult;


/**
 * Return collection statistics from mongo
 *
 * @since 3.0
 */
public class GetStatisticsOperation implements AsyncReadOperation<Document>, ReadOperation<Document> {

    private final MongoNamespace collectionNamespace;
    private final Document collStatsCommand;

    public GetStatisticsOperation(final MongoNamespace collectionNamespace) {
        this.collectionNamespace = collectionNamespace;
        collStatsCommand = new Document("collStats", collectionNamespace.getCollectionName());
    }

    @Override
    public Document execute(final ReadBinding binding) {
        CommandResult result;
        try {
            result = executeWrappedCommandProtocol(collectionNamespace, collStatsCommand, binding);
        } catch (MongoCommandFailureException e) {
            result = ignoreNameSpaceErrors(e);
        }
        return transformer().apply(result);
    }

    @Override
    public MongoFuture<Document> executeAsync(final AsyncReadBinding binding) {
        MongoFuture<CommandResult> futureCommandResult = executeWrappedCommandProtocolAsync(collectionNamespace, collStatsCommand, binding);
        return transformResult(ignoreNameSpaceErrors(futureCommandResult), transformer());
    }

    private Function<CommandResult, Document> transformer() {
        return new Function<CommandResult, Document>() {
            @Override
            public Document apply(final CommandResult result) {
                return result.getResponse();
            }
        };
    }
}
