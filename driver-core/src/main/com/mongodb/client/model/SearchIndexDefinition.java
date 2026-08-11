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

package com.mongodb.client.model;

import com.mongodb.annotations.Sealed;
import org.bson.conversions.Bson;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.assertions.Assertions.notNullElements;
import static java.util.Arrays.asList;

/**
 * A definition for an Atlas Search index.
 *
 * <p>This interface provides factory methods for creating search index definitions
 * that can be passed to {@link SearchIndexModel}.</p>
 *
 * @see SearchIndexModel
 * @see VectorSearchIndexDefinition
 * @since 5.8
 */
@Sealed
public interface SearchIndexDefinition extends Bson {

    /**
     * Creates a vector search index definition with the specified fields.
     *
     * <p>The resulting definition produces a document of the form {@code {"fields": [...]}},
     * suitable for use with {@link SearchIndexType#vectorSearch()}.</p>
     *
     * @param fields the fields for the vector search index. Each field should be created using
     *               {@link VectorSearchIndexFields} factory methods, or may be a raw {@link Bson} document.
     * @return a new {@link VectorSearchIndexDefinition}
     * @see VectorSearchIndexFields#vectorField(String)
     * @see VectorSearchIndexFields#filterField(String)
     * @see VectorSearchIndexFields#autoEmbedField(String)
     * @since 5.8
     */
    static VectorSearchIndexDefinition vectorSearch(final Bson... fields) {
        List<Bson> fieldList = asList(notNull("fields", fields));
        isTrueArgument("fields must not be empty", !fieldList.isEmpty());
        notNullElements("fields", fieldList);
        return new VectorSearchIndexDefinition(fieldList);
    }

    /**
     * Creates a vector search index definition with the specified fields.
     *
     * <p>The resulting definition produces a document of the form {@code {"fields": [...]}},
     * suitable for use with {@link SearchIndexType#vectorSearch()}.</p>
     *
     * @param fields the fields for the vector search index. Each field should be created using
     *               {@link VectorSearchIndexFields} factory methods, or may be a raw {@link Bson} document.
     * @return a new {@link VectorSearchIndexDefinition}
     * @see VectorSearchIndexFields#vectorField(String)
     * @see VectorSearchIndexFields#filterField(String)
     * @see VectorSearchIndexFields#autoEmbedField(String)
     * @since 5.8
     */
    static VectorSearchIndexDefinition vectorSearch(final List<? extends Bson> fields) {
        notNullElements("fields", fields);
        isTrueArgument("fields must not be empty", !fields.isEmpty());
        return new VectorSearchIndexDefinition(fields);
    }
}
