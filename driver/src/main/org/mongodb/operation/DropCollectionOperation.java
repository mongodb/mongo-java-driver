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

package org.mongodb.operation;

import org.mongodb.Codec;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

/**
 * Operation to drop a Collection in MongoDB.  The {@code execute} method throws MongoCommandFailureException if something goes wrong, but
 * it will not throw an Exception if the collection does not exist before trying to drop it.
 */
public class DropCollectionOperation extends BaseOperation<CommandResult> {
    private final MongoNamespace namespace;
    private final Document dropCollectionCommand;
    private final Codec<Document> commandCodec = new DocumentCodec();

    /**
     * Create the Operation to drop a Collection from MongoDB.
     *
     * @param namespace      the database/collection namespace for the collection to be dropped
     * @param bufferProvider the BufferProvider to use
     * @param session        the Session to use
     * @param closeSession   true of you want this operation to close the session after completion
     */
    public DropCollectionOperation(final MongoNamespace namespace, final BufferProvider bufferProvider, final Session session,
                                   final boolean closeSession) {
        super(bufferProvider, session, closeSession);
        this.namespace = namespace;
        dropCollectionCommand = new Document("drop", namespace.getCollectionName());
    }

    @Override
    public CommandResult execute() {
        try {
            ServerConnectionProvider provider = getPrimaryServerConnectionProvider();
            return new CommandProtocol(namespace.getDatabaseName(), dropCollectionCommand, commandCodec, commandCodec, getBufferProvider(),
                                       provider.getServerDescription(), provider.getConnection(), true).execute();
        } catch (MongoCommandFailureException e) {
            return ignoreNamespaceNotFoundExceptionsWhenDroppingACollection(e);
        }
    }

    private CommandResult ignoreNamespaceNotFoundExceptionsWhenDroppingACollection(final MongoCommandFailureException e) {
        if (!e.getCommandResult().getErrorMessage().equals("ns not found")) {
            throw e;
        }
        return e.getCommandResult();
    }

}
