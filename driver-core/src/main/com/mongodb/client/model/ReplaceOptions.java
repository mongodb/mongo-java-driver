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

import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * The options to apply when replacing documents.
 *
 * @since 3.7
 * @mongodb.driver.manual tutorial/modify-documents/ Updates
 * @mongodb.driver.manual reference/operator/update/ Update Operators
 * @mongodb.driver.manual reference/command/update/ Update Command
 */
public class ReplaceOptions {
    private boolean upsert;
    private Boolean bypassDocumentValidation;
    private Collation collation;

    /**
     * Creates replace options from updateOptions.
     *
     * @param updateOptions the updateOptions
     * @return replace options
     */
    public static ReplaceOptions createReplaceOptions(final UpdateOptions updateOptions) {
        notNull("updateOptions", updateOptions);

        List<? extends Bson> arrayFilters = updateOptions.getArrayFilters();
        isTrue("ArrayFilters should be empty.",  arrayFilters == null || arrayFilters.isEmpty());

        return new ReplaceOptions()
                .bypassDocumentValidation(updateOptions.getBypassDocumentValidation())
                .collation(updateOptions.getCollation())
                .upsert(updateOptions.isUpsert());
    }

    /**
     * Returns true if a new document should be inserted if there are no matches to the query filter.  The default is false.
     *
     * @return true if a new document should be inserted if there are no matches to the query filter
     */
    public boolean isUpsert() {
        return upsert;
    }

    /**
     * Set to true if a new document should be inserted if there are no matches to the query filter.
     *
     * @param upsert true if a new document should be inserted if there are no matches to the query filter
     * @return this
     */
    public ReplaceOptions upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    /**
     * Gets the the bypass document level validation flag
     *
     * @return the bypass document level validation flag
     * @mongodb.server.release 3.2
     */
    @Nullable
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * Sets the bypass document level validation flag.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @mongodb.server.release 3.2
     */
    public ReplaceOptions bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @mongodb.server.release 3.4
     */
    @Nullable
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @mongodb.server.release 3.4
     */
    public ReplaceOptions collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public String toString() {
        return "ReplaceOptions{"
                + "upsert=" + upsert
                + ", bypassDocumentValidation=" + bypassDocumentValidation
                + ", collation=" + collation
                + '}';
    }
}
