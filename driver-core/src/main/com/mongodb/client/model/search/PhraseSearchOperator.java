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
 * @see SearchOperator#phrase(SearchPath, String)
 * @see SearchOperator#phrase(Iterable, Iterable)
 * @since 5.3
 */

@Sealed
@Beta(Reason.CLIENT)
public interface PhraseSearchOperator extends SearchOperator {
    @Override
    PhraseSearchOperator score(SearchScore modifier);

    /**
     * Creates a new {@link PhraseSearchOperator} that uses slop. The default value is 0.
     *
     * @param slop The allowable distance between words in the query phrase.
     * @return A new {@link PhraseSearchOperator}.
     */
    PhraseSearchOperator slop(int slop);

    /**
     * Creates a new {@link PhraseSearchOperator} that uses synonyms.
     *
     * @param name The name of the synonym mapping.
     * @return A new {@link PhraseSearchOperator}.
     *
     * @mongodb.atlas.manual atlas-search/synonyms/ Synonym mappings
     */
    PhraseSearchOperator synonyms(String name);
}
