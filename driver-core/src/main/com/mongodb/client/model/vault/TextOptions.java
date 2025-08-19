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

package com.mongodb.client.model.vault;

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Reason;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

/**
 * Text options for a Queryable Encryption field that supports text queries.
 *
 * <p>Note: TextOptions is in Alpha and subject to backwards breaking changes.
 *
 * @since 5.6
 * @mongodb.server.release 8.2
 * @mongodb.driver.manual /core/queryable-encryption/ queryable encryption
 */
@Alpha(Reason.SERVER)
public class TextOptions {
    private Boolean caseSensitive;
    private Boolean diacriticSensitive;
    @Nullable
    private BsonDocument prefixOptions;
    @Nullable
    private BsonDocument suffixOptions;
    @Nullable
    private BsonDocument substringOptions;

    /**
     * Construct a new instance
     */
    public TextOptions() {
    }

    /**
     * @return true if text indexes for this field are case sensitive.
     */
    public boolean getCaseSensitive() {
        return caseSensitive;
    }

    /**
     * Set case sensitivity
     *
     * @param caseSensitive true if text indexes are case sensitive
     * @return this
     */
    public TextOptions caseSensitive(final boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
        return this;
    }

    /**
     * @return true if text indexes are diacritic sensitive
     */
    public boolean getDiacriticSensitive() {
        return diacriticSensitive;
    }

    /**
     * Set diacritic sensitivity
     *
     * @param diacriticSensitive true if text indexes are diacritic sensitive
     * @return this
     */
    public TextOptions diacriticSensitive(final boolean diacriticSensitive) {
        this.diacriticSensitive = diacriticSensitive;
        return this;
    }

    /**
     * Set the prefix options.
     *
     * <p>Expected to be a {@link BsonDocument} in the format of:</p>
     *
     * <pre>
     * {@code
     *   {
     *    // strMinQueryLength is the minimum allowed query length. Querying with a shorter string will error.
     *    strMinQueryLength: BsonInt32,
     *    // strMaxQueryLength is the maximum allowed query length. Querying with a longer string will error.
     *    strMaxQueryLength: BsonInt32
     *   }
     * }
     * </pre>
     *
     * @param prefixOptions the prefix options or null
     * @return this
     */
    public TextOptions prefixOptions(@Nullable final BsonDocument prefixOptions) {
        this.prefixOptions = prefixOptions;
        return this;
    }

    /**
     * @see #prefixOptions(BsonDocument)
     * @return the prefix options document or null
     */
    @Nullable
    public BsonDocument getPrefixOptions() {
        return prefixOptions;
    }

    /**
     * Set the suffix options.
     *
     * <p>Expected to be a {@link BsonDocument} in the format of:</p>
     *
     * <pre>
     * {@code
     *   {
     *    // strMinQueryLength is the minimum allowed query length. Querying with a shorter string will error.
     *    strMinQueryLength: BsonInt32,
     *    // strMaxQueryLength is the maximum allowed query length. Querying with a longer string will error.
     *    strMaxQueryLength: BsonInt32
     *   }
     * }
     * </pre>
     *
     * @param suffixOptions the suffix options or null
     * @return this
     */
    public TextOptions suffixOptions(@Nullable final BsonDocument suffixOptions) {
        this.suffixOptions = suffixOptions;
        return this;
    }

    /**
     * @see #suffixOptions(BsonDocument)
     * @return the suffix options document or null
     */
    @Nullable
    public BsonDocument getSuffixOptions() {
        return suffixOptions;
    }

    /**
     * Set the substring options.
     *
     * <p>Expected to be a {@link BsonDocument} in the format of:</p>
     *
     * <pre>
     * {@code
     *   {
     *    // strMaxLength is the maximum allowed length to insert. Inserting longer strings will error.
     *    strMaxLength: BsonInt32,
     *    // strMinQueryLength is the minimum allowed query length. Querying with a shorter string will error.
     *    strMinQueryLength: BsonInt32,
     *    // strMaxQueryLength is the maximum allowed query length. Querying with a longer string will error.
     *    strMaxQueryLength: BsonInt32
     *   }
     * }
     * </pre>
     *
     * @param substringOptions the substring options or null
     * @return this
     */
    public TextOptions substringOptions(@Nullable final BsonDocument substringOptions) {
        this.substringOptions = substringOptions;
        return this;
    }

    /**
     * @see #substringOptions(BsonDocument)
     * @return the substring options document or null
     */
    @Nullable
    public BsonDocument getSubstringOptions() {
        return substringOptions;
    }

}
