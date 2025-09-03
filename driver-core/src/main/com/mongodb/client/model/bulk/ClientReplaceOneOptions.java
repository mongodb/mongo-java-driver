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
package com.mongodb.client.model.bulk;

import com.mongodb.annotations.Sealed;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.client.model.bulk.ConcreteClientReplaceOneOptions;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

/**
 * The options to apply when replacing a document.
 *
 * @since 5.3
 */
@Sealed
public interface ClientReplaceOneOptions extends BaseClientWriteModelOptions, BaseClientUpsertableWriteModelOptions {
    /**
     * Creates the default options.
     *
     * @return The default options.
     */
    static ClientReplaceOneOptions clientReplaceOneOptions() {
        return new ConcreteClientReplaceOneOptions();
    }

    /**
     * Sets the collation.
     *
     * @param collation The collation. {@code null} represents the server default.
     * @return {@code this}.
     */
    @Override
    ClientReplaceOneOptions collation(@Nullable Collation collation);

    /**
     * Sets the index specification,
     * {@code null}-ifies {@linkplain #hintString(String) hint string}.
     *
     * @param hint The index specification. {@code null} represents the server default.
     * @return {@code this}.
     */
    @Override
    ClientReplaceOneOptions hint(@Nullable Bson hint);

    /**
     * Sets the index name,
     * {@code null}-ifies {@linkplain #hint(Bson) hint}.
     *
     * @param hintString The index name. {@code null} represents the server default.
     * @return {@code this}.
     */
    @Override
    ClientReplaceOneOptions hintString(@Nullable String hintString);

    /**
     * Enables or disables creation of a document if no documents match the filter.
     *
     * @param upsert The upsert flag. {@code null} represents the server default.
     * @return {@code this}.
     */
    @Override
    ClientReplaceOneOptions upsert(@Nullable Boolean upsert);

    /**
     * Sets the sort criteria to apply to the operation. A null value means no sort criteria is set.
     *
     * <p>
     * The sort criteria determines which document the operation replaces if the query matches multiple documents.
     * The first document matched by the specified sort criteria will be replaced.
     *
     * @param sort The sort criteria. {@code null} represents the server default.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.replaceOne/ Sort
     * @mongodb.server.release 8.0
     * @since 5.4
     */
    ClientReplaceOneOptions sort(@Nullable Bson sort);
}
