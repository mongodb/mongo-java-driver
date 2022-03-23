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
package com.mongodb.client.model.search;

import com.mongodb.annotations.Evolving;
import com.mongodb.client.model.ToBsonValue;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.client.model.search.ConstructibleBsonToSearchPathAdapter.VALUE_KEY;

/**
 * A specification of document fields to be searched.
 *
 * @mongodb.atlas.manual atlas-search/path-construction/ Path
 * @mongodb.driver.manual core/document/#dot-notation Dot notation
 * @since 4.6
 */
@Evolving
public interface SearchPath extends ToBsonValue {
    /**
     * Returns a {@link SearchPath} for the given {@code path}.
     *
     * @param path The name of the field to search. Must not contain {@linkplain #wildcardPath(String) wildcard} characters.
     * @return The requested {@link SearchPath}.
     * @mongodb.driver.manual core/document/#dot-notation Dot notation
     */
    static FieldSearchPath fieldPath(final String path) {
        notNull("path", path);
        if (path.contains("*")) {
            throw new IllegalArgumentException("path must not contain '*'");
        }
        return new ConstructibleBsonToSearchPathAdapter(new BsonDocument(VALUE_KEY, new BsonString(path)));
    }

    /**
     * Returns a {@link SearchPath} for the given {@code wildcardPath}.
     *
     * @param wildcardPath The specification of the fields to search that contains wildcard ({@code '*'}) characters.
     * Must not contain {@code '**'}.
     * @return The requested {@link SearchPath}.
     * @mongodb.driver.manual core/document/#dot-notation Dot notation
     */
    static WildcardSearchPath wildcardPath(final String wildcardPath) {
        notNull("wildcardPath", wildcardPath);
        if (wildcardPath.contains("**")) {
            throw new IllegalArgumentException("wildcardPath must not contain '**'");
        }
        if (!wildcardPath.contains("*")) {
            throw new IllegalArgumentException("wildcardPath must contain '*'");
        }
        return new ConstructibleBsonToSearchPathAdapter(new BsonDocument("wildcard", new BsonString(wildcardPath)));
    }
}
