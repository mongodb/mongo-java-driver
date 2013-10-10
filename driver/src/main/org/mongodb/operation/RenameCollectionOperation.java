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
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

import static org.mongodb.MongoNamespace.asNamespaceString;

/**
 * Executing this operation will rename the given collection to the new name.  If the new name is the same as an existing collection and
 * dropTarget is true, this existing collection will be dropped. If dropTarget is false and the newCollectionName is the same as an existing
 * collection, a MongoServerException with error code 10027 will be thrown.
 */
public class RenameCollectionOperation extends BaseOperation<CommandResult> {
    private final Codec<Document> commandCodec = new DocumentCodec();
    private final String originalCollectionName;
    private final String newCollectionName;
    private final boolean dropTarget;
    private final String databaseName;

    /**
     * The constructor of this abstract class takes the fields that are required by all basic operations.
     *
     * @param bufferProvider         the BufferProvider to use when reading or writing to the network
     * @param session                the current Session, which will give access to a connection to the MongoDB instance
     * @param closeSession           true if the session should be closed at the end of the execute method
     * @param databaseName           the name of the database containing the collection to rename
     * @param originalCollectionName the name of the collection to rename
     * @param newCollectionName      the desired new name for the collection
     * @param dropTarget             set to true if you want any existing database with newCollectionName to be dropped during the rename
     */
    public RenameCollectionOperation(final BufferProvider bufferProvider, final Session session, final boolean closeSession,
                                     final String databaseName, final String originalCollectionName, final String newCollectionName,
                                     final boolean dropTarget) {
        super(bufferProvider, session, closeSession);
        this.originalCollectionName = originalCollectionName;
        this.newCollectionName = newCollectionName;
        this.dropTarget = dropTarget;
        this.databaseName = databaseName;
    }

    /**
     * Rename the collection with {@code oldCollectionName} in database {@code databaseName} to the {@code newCollectionName}.
     *
     * @return a CommandResult containing the success or failure of executing the rename.
     * @throws org.mongodb.MongoServerException
     *          with code 10027 if you provide a newCollectionName that is the name of an existing collection and dropTarget is false, with
     *          code 10026 if the oldCollectionName is the name of a collection that doesn't exist
     */
    @Override
    public CommandResult execute() {
        ServerConnectionProvider provider = getPrimaryServerConnectionProvider();
        return new CommandProtocol("admin", createCommand(), commandCodec, commandCodec,
                                   getBufferProvider(), provider.getServerDescription(), provider.getConnection(), false)
                   .execute();
    }

    private Document createCommand() {
        return new Document("renameCollection", asNamespaceString(databaseName, originalCollectionName))
                   .append("to", asNamespaceString(databaseName, newCollectionName))
                   .append("dropTarget", dropTarget);
    }

}
