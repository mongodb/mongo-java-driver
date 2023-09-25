/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.internal.TimeoutSettings;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;

/**
 * An operation that updates an Atlas Search index.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
final class UpdateSearchIndexesOperation extends AbstractWriteSearchIndexOperation {
    private static final String COMMAND_NAME = "updateSearchIndex";
    private final SearchIndexRequest request;

    UpdateSearchIndexesOperation(final TimeoutSettings timeoutSettings, final MongoNamespace namespace, final SearchIndexRequest request,
                                 final WriteConcern writeConcern) {
        super(timeoutSettings, namespace, writeConcern);
        this.request = request;
    }

    @Override
    BsonDocument buildCommand() {
        BsonDocument command = new BsonDocument(COMMAND_NAME, new BsonString(getNamespace().getCollectionName()))
                .append("name", new BsonString(request.getIndexName()))
                .append("definition", request.getDefinition());
        appendWriteConcernToCommand(getWriteConcern(), command);
        return command;
    }
}

