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

import com.mongodb.client.model.SearchIndexType;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import static com.mongodb.assertions.Assertions.assertNotNull;

/**
 * A request for creating or updating an Atlas Search index.
 *
 * <p>Additional options may be introduced as Atlas evolves.
 * To maintain a clear API, it can be split into separate classes, e.g., {@code SearchIndexCreateRequest}
 * and {@code SearchIndexUpdateRequest}, for handling each operation separately in the future.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
final class SearchIndexRequest {
    private final BsonDocument definition;
    @Nullable
    private final String indexName;
    @Nullable
    private final SearchIndexType searchIndexType;

    SearchIndexRequest(final BsonDocument definition, @Nullable final String indexName, final SearchIndexType searchIndexType) {
        assertNotNull(definition);
        this.definition = definition;
        this.indexName = indexName;
        this.searchIndexType = searchIndexType;
    }

    SearchIndexRequest(final BsonDocument definition, @Nullable final String indexName) {
        assertNotNull(definition);
        this.definition = definition;
        this.indexName = indexName;
        this.searchIndexType = null;
    }

    public BsonDocument getDefinition() {
        return definition;
    }

    @Nullable
    public String getIndexName() {
        return indexName;
    }
    @Nullable
    public SearchIndexType getSearchIndexType() {
        return searchIndexType;
    }

}
