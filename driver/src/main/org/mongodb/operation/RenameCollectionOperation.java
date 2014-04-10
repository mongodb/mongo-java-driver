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
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.session.Session;

import static org.mongodb.MongoNamespace.asNamespaceString;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocol;

/**
 * Executing this operation will rename the given collection to the new name.  If the new name is the same as an existing collection and
 * dropTarget is true, this existing collection will be dropped. If dropTarget is false and the newCollectionName is the same as an existing
 * collection, a MongoServerException will be thrown.
 */
public class RenameCollectionOperation implements Operation<CommandResult> {
    private final Codec<Document> commandCodec = new DocumentCodec();
    private final String originalCollectionName;
    private final String newCollectionName;
    private final boolean dropTarget;
    private final String databaseName;

    /**
     * The constructor of this abstract class takes the fields that are required by all basic operations.
     * @param databaseName           the name of the database containing the collection to rename
     * @param originalCollectionName the name of the collection to rename
     * @param newCollectionName      the desired new name for the collection
     * @param dropTarget             set to true if you want any existing database with newCollectionName to be dropped during the rename
     */
    public RenameCollectionOperation(final String databaseName, final String originalCollectionName, final String newCollectionName,
                                     final boolean dropTarget) {
        super();
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
     *          if you provide a newCollectionName that is the name of an existing collection and dropTarget is false,
     *          or if the oldCollectionName is the name of a collection that doesn't exist
     * @param session
     */
    @Override
    public CommandResult execute(final Session session) {
        return executeWrappedCommandProtocol("admin", createCommand(), commandCodec, commandCodec, session);
    }

    private Document createCommand() {
        return new Document("renameCollection", asNamespaceString(databaseName, originalCollectionName))
                   .append("to", asNamespaceString(databaseName, newCollectionName))
                   .append("dropTarget", dropTarget);
    }

}
