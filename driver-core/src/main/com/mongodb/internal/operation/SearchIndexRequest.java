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

import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The settings to apply to the creation of a search index.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class SearchIndexRequest {
    private BsonDocument definition;
    private String searchIndexName;

    @Nullable
    public BsonDocument getDefinition() {
        return definition;
    }

    public void setDefinition(final BsonDocument definition) {
        notNull("definition", definition);
        this.definition = definition;
    }

   @Nullable
    public String getSearchIndexName() {
        return searchIndexName;
    }

    public void setSearchIndexName(@Nullable final String searchIndexName) {
        this.searchIndexName = searchIndexName;
    }
}
