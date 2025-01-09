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
import com.mongodb.internal.client.model.bulk.ConcreteClientDeleteOneOptions;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

/**
 * The options to apply when deleting a document.
 *
 * @since 5.3
 */
@Sealed
public interface ClientDeleteOneOptions extends BaseClientDeleteOptions {
    /**
     * Creates the default options.
     *
     * @return The default options.
     */
    static ClientDeleteOneOptions clientDeleteOneOptions() {
        return new ConcreteClientDeleteOneOptions();
    }

    /**
     * Sets the collation.
     *
     * @param collation The collation. {@code null} represents the server default.
     * @return {@code this}.
     */
    @Override
    ClientDeleteOneOptions collation(@Nullable Collation collation);

    /**
     * Sets the index specification,
     * {@code null}-ifies {@linkplain #hintString(String) hint string}.
     *
     * @param hint The index specification. {@code null} represents the server default.
     * @return {@code this}.
     */
    @Override
    ClientDeleteOneOptions hint(@Nullable Bson hint);

    /**
     * Sets the index name,
     * {@code null}-ifies {@linkplain #hint(Bson) hint}.
     *
     * @param hintString The index name. {@code null} represents the server default.
     * @return {@code this}.
     */
    @Override
    ClientDeleteOneOptions hintString(@Nullable String hintString);
}
