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

import org.mongodb.Codec;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.session.Session;

import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocolAsync;
import static org.mongodb.operation.OperationHelper.ignoreNameSpaceErrors;

/**
 * Operation to drop a Collection in MongoDB.  The {@code execute} method throws MongoCommandFailureException if something goes wrong, but
 * it will not throw an Exception if the collection does not exist before trying to drop it.
 */
public class DropCollectionOperation implements AsyncOperation<CommandResult>, Operation<CommandResult> {
    private final MongoNamespace namespace;
    private final Document dropCollectionCommand;
    private final Codec<Document> commandCodec = new DocumentCodec();

    /**
     * Create the Operation to drop a Collection from MongoDB.
     *
     * @param namespace the database/collection namespace for the collection to be dropped
     */
    public DropCollectionOperation(final MongoNamespace namespace) {
        this.namespace = namespace;
        dropCollectionCommand = new Document("drop", namespace.getCollectionName());
    }

    @Override
    public CommandResult execute(final Session session) {
        try {
            return executeWrappedCommandProtocol(namespace.getDatabaseName(), dropCollectionCommand, commandCodec, commandCodec, session);
        } catch (MongoCommandFailureException e) {
            return ignoreNameSpaceErrors(e);
        }
    }

    @Override
    public MongoFuture<CommandResult> executeAsync(final Session session) {
        MongoFuture<CommandResult> fututeDropOperation = executeWrappedCommandProtocolAsync(namespace.getDatabaseName(),
                dropCollectionCommand, commandCodec, commandCodec, session);
        return ignoreNameSpaceErrors(fututeDropOperation);
    }


}
