/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

import org.mongodb.CommandDocument;
import org.mongodb.MongoCollection;
import org.mongodb.operation.MongoCommand;
import org.mongodb.operation.MongoFindAndUpdate;

import static org.mongodb.command.CommandDocumentTemplates.getFindAndModify;

public final class FindAndUpdate<T> extends MongoCommand {

    public FindAndUpdate(final MongoCollection<T> collection, final MongoFindAndUpdate<T> findAndUpdate) {
        super(asCommandDocument(findAndUpdate, collection.getName()));
    }

    private static <T> CommandDocument asCommandDocument(final MongoFindAndUpdate<T> findAndUpdate,
                                                         final String collectionName) {
        final CommandDocument cmd = getFindAndModify(findAndUpdate, collectionName);
        cmd.put("update", findAndUpdate.getUpdateOperations());
        return cmd;
    }
}
