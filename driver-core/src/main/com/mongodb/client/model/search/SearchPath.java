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
import com.mongodb.internal.client.model.Util;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.client.model.Util.SEARCH_PATH_VALUE_KEY;

/**
 * A specification of document fields to be searched.
 * <p>
 * Depending on the context, one of the following methods may be used to get a representation of a {@link SearchPath}
 * with the correct syntax: {@link #toBsonDocument()}, {@link #toBsonValue()}, {@link FieldSearchPath#toValue()}.</p>
 *
 * @mongodb.atlas.manual atlas-search/path-construction/ Path
 * @since 4.7
 */
@Evolving
public interface SearchPath extends Bson {
    /**
     * Returns a {@link SearchPath} for the given {@code path}.
     *
     * @param path The name of the field to search. Must not contain {@linkplain #wildcardPath(String) wildcard} characters.
     * @return The requested {@link SearchPath}.
     * @mongodb.driver.manual core/document/#dot-notation Dot notation
     */
    static FieldSearchPath fieldPath(final String path) {
        notNull("path", path);
        isTrueArgument("path must not contain '*'", !path.contains("*"));
        return new SearchConstructibleBson(new BsonDocument(SEARCH_PATH_VALUE_KEY, new BsonString(path)));
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
        isTrueArgument("wildcardPath must contain '*'", wildcardPath.contains("*"));
        isTrueArgument("wildcardPath must not contain '**'", !wildcardPath.contains("**"));
        return new SearchConstructibleBson(new BsonDocument("wildcard", new BsonString(wildcardPath)));
    }

    /**
     * Converts this object to {@link BsonValue}.
     * If {@link #toBsonDocument()} contains only the {@value Util#SEARCH_PATH_VALUE_KEY} key,
     * then returns {@link BsonString} representing the value of this key,
     * otherwise returns {@link #toBsonDocument()}.
     * <p>
     * This method may be useful when using the {@code of} methods, e.g., {@link SearchHighlight#of(Bson)}.
     * Depending on the syntax of the document being constructed,
     * it may be required to use the method {@link FieldSearchPath#toValue()} instead.</p>
     *
     * @return A {@link BsonValue} representing this {@link SearchPath}.
     * @see FieldSearchPath#toValue()
     */
    default BsonValue toBsonValue() {
        final BsonDocument doc = toBsonDocument();
        if (doc.size() > 1) {
            return doc;
        } else {
            final BsonString value = doc.getString(SEARCH_PATH_VALUE_KEY, null);
            if (value != null) {
                // paths that contain only `SEARCH_PATH_VALUE_KEY` must be represented as a `BsonString`
                return value;
            } else {
                return doc;
            }
        }
    }
}
