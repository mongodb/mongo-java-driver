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
import com.mongodb.client.model.SearchIndexType;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.assertions.Assertions.assertNotNull;

/**
 * An operation that creates one or more Atlas Search indexes.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class CreateSearchIndexesOperation extends AbstractWriteSearchIndexOperation {
    private static final String COMMAND_NAME = "createSearchIndexes";
    private final List<SearchIndexRequest> indexRequests;

    public CreateSearchIndexesOperation(final MongoNamespace namespace, final List<SearchIndexRequest> indexRequests) {
        super(namespace);
        this.indexRequests = assertNotNull(indexRequests);
    }

    private static BsonArray convert(final List<SearchIndexRequest> requests) {
        return requests.stream()
                .map(CreateSearchIndexesOperation::convert)
                .collect(Collectors.toCollection(BsonArray::new));
    }

    private static BsonDocument convert(final SearchIndexRequest request) {
        BsonDocument bsonIndexRequest = new BsonDocument();
        String searchIndexName = request.getIndexName();
        if (searchIndexName != null) {
            bsonIndexRequest.append("name", new BsonString(searchIndexName));
        }
        SearchIndexType searchIndexType = request.getSearchIndexType();
        if (searchIndexType != null) {
            bsonIndexRequest.append("type", searchIndexType.toBsonValue());
        }
        bsonIndexRequest.append("definition", request.getDefinition());
        return bsonIndexRequest;
    }

    @Override
    BsonDocument buildCommand() {
        return new BsonDocument(COMMAND_NAME, new BsonString(getNamespace().getCollectionName()))
                .append("indexes", convert(indexRequests));
    }
}
