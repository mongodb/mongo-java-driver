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

import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import static com.mongodb.assertions.Assertions.assertNotNull;

/**
 * The settings to apply to the creation of an Atlas Search index.
 *
 * <p>Note: This class is semantically equivalent to {@link SearchIndexModel}.</p>
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
final class SearchIndexRequest {
    private final BsonDocument definition;
    @Nullable
    private final String indexName;

    SearchIndexRequest(final BsonDocument definition, @Nullable final String indexName) {
        assertNotNull(definition);
        this.definition = definition;
        this.indexName = indexName;
    }

    public BsonDocument getDefinition() {
        return definition;
    }

    @Nullable
    public String getIndexName() {
        return indexName;
    }
}
