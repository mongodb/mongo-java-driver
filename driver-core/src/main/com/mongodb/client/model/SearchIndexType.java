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
import org.bson.BsonString;
import org.bson.BsonValue;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * This interface represents an Atlas Search Index type, which is utilized for creating specific types of indexes.
 * <p>
 * It provides methods for creating and converting Atlas Search Index types to {@link BsonValue}.
 * </p>
 *
 * @mongodb.server.release 7.0
 * @see SearchIndexModel The model class that utilizes this index type.
 * @since 5.2
 */
@Sealed
public interface SearchIndexType {

    /**
     * Returns a {@link SearchIndexType} instance representing the "search" index type.
     *
     * @return The requested {@link SearchIndexType}.
     */
    static SearchIndexType search() {
        return new SearchIndexTypeBson(new BsonString("search"));
    }

    /**
     * Returns a {@link SearchIndexType} instance representing the "vectorSearch" index type.
     *
     * @return The requested {@link SearchIndexType}.
     */
    static SearchIndexType vectorSearch() {
        return new SearchIndexTypeBson(new BsonString("vectorSearch"));
    }

    /**
     * Creates a {@link SearchIndexType} from a {@link BsonValue} in situations when there is no builder method
     * that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link SearchIndexType}s,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  SearchIndexType type1 = SearchIndexType.vectorSearch();
     *  SearchIndexType type2 = SearchIndexType.of(new BsonString("vectorSearch"));
     * }</pre>
     *
     * @param indexType A {@link BsonValue} representing the required {@link SearchIndexType}.
     * @return The requested {@link SearchIndexType}.
     */
    static SearchIndexType of(final BsonValue indexType) {
        notNull("indexType", indexType);
        return new SearchIndexTypeBson(indexType);
    }

    /**
     * Converts this object to {@link BsonValue}.
     *
     * @return A {@link BsonValue} representing this {@link SearchIndexType}.
     */
    BsonValue toBsonValue();
}
