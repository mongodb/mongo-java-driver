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

package com.mongodb.client.model.mql;

import com.mongodb.annotations.Beta;
import com.mongodb.annotations.Sealed;

/**
 * A map entry {@linkplain MqlValue value} in the context
 * of the MongoDB Query Language (MQL). An entry has a
 * {@linkplain MqlString string} key and some
 * {@linkplain MqlValue value}. Entries are used with
 * {@linkplain MqlMap maps}.
 *
 * <p>Entries are {@linkplain MqlValue#isDocumentOr document-like} and
 * {@linkplain MqlValue#isMapOr map-like}, unless the method returning the
 * entry specifies otherwise.
 *
 * @param <T> The type of the value
 * @since 4.9.0
 */
@Sealed
@Beta(Beta.Reason.CLIENT)
public interface MqlEntry<T extends MqlValue> extends MqlValue {

    /**
     * The key of {@code this} entry.
     *
     * @mongodb.server.release 5.0
     * @return the key.
     */
    MqlString getKey();

    /**
     * The value of {@code this} entry.
     *
     * @mongodb.server.release 5.0
     * @return the value.
     */
    T getValue();

    /**
     * An entry with the same key as {@code this} entry, and the
     * specified {@code value}.
     *
     * @mongodb.server.release 5.0
     * @param value the value.
     * @return the resulting entry.
     */
    MqlEntry<T> setValue(T value);

    /**
     * An entry with the same value as {@code this} entry, and the
     * specified {@code key}.
     *
     * @mongodb.server.release 5.0
     * @param key the key.
     * @return the resulting entry.
     */
    MqlEntry<T> setKey(MqlString key);
}
