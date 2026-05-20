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

import com.mongodb.annotations.Beta;
import com.mongodb.annotations.Reason;
import com.mongodb.annotations.Sealed;

/**
 * A {@link SearchOperator} that performs vector search within the {@code $search} pipeline stage.
 *
 * @mongodb.atlas.manual atlas-search/operators-and-collectors/#operators Search operators
 * @since 5.8
 */
@Sealed
@Beta(Reason.CLIENT)
public interface VectorSearchOperator extends SearchOperator {

    /**
     * Creates a new {@link VectorSearchOperator} with the filter specified.
     *
     * @param filter A search operator to filter documents.
     * @return A new {@link VectorSearchOperator}.
     */
    VectorSearchOperator filter(SearchOperator filter);

    /**
     * Creates a new {@link VectorSearchOperator} with the scoring modifier specified.
     *
     * @param modifier The scoring modifier.
     * @return A new {@link VectorSearchOperator}.
     */
    @Override
    VectorSearchOperator score(SearchScore modifier);
}
