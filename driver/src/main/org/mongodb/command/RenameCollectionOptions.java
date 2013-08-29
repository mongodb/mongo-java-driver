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

import org.mongodb.Document;

import static org.mongodb.MongoNamespace.asNamespaceString;

public class RenameCollectionOptions {
    private final String originalCollectionName;
    private final String newCollectionName;
    private final boolean dropTarget;

    public RenameCollectionOptions(final String originalCollectionName, final String newCollectionName) {
        this(originalCollectionName, newCollectionName, false);
    }

    public RenameCollectionOptions(final String originalCollectionName, final String newCollectionName,
                                   final boolean dropTarget) {
        this.originalCollectionName = originalCollectionName;
        this.newCollectionName = newCollectionName;
        this.dropTarget = dropTarget;
    }

    public Document toDocument(final String databaseName) {
        return new Document("renameCollection", asNamespaceString(databaseName, originalCollectionName))
               .append("to", asNamespaceString(databaseName, newCollectionName))
               .append("dropTarget", dropTarget);
    }
}
