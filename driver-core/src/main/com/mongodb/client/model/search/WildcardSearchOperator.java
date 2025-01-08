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
 * @see SearchOperator#wildcard(String, SearchPath)
 * @see SearchOperator#wildcard(Iterable, Iterable)
 * @since 4.7
 */
@Sealed
@Beta(Reason.CLIENT)
public interface WildcardSearchOperator extends SearchOperator {
    @Override
    WildcardSearchOperator score(SearchScore modifier);

    /**
     * Creates a new {@link WildcardSearchOperator} that runs against an analyzed field.
     *
     * <p> Must be set to true if the query is run against an analyzed field. </p>
     *
     * @return A new {@link WildcardSearchOperator}.
     */
    WildcardSearchOperator allowAnalyzedField();

    /**
     * Creates a new {@link WildcardSearchOperator} that runs against an analyzed field. The default value is false.
     *
     * <p> Must be set to true if the query is run against an analyzed field. </p>
     *
     * @param allowAnalyzedField The boolean value that sets if the query should run against an analyzed field.
     *
     * @return A new {@link WildcardSearchOperator}.
     */
    WildcardSearchOperator allowAnalyzedField(boolean allowAnalyzedField);
}
