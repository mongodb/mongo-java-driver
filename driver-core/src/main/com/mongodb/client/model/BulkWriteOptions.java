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
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

/**
 * The options to apply to a bulk write.
 *
 * @since 3.0
 */
public final class BulkWriteOptions {
    private boolean ordered = true;
    private Boolean bypassDocumentValidation;
    private BsonValue comment;
    private Bson variables;

    /**
     * If true, then when a write fails, return without performing the remaining
     * writes. If false, then when a write fails, continue with the remaining writes, if any.
     * Defaults to true.
     *
     * @return true if the writes are ordered
     */
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * If true, then when a write fails, return without performing the remaining
     * writes. If false, then when a write fails, continue with the remaining writes, if any.
     * Defaults to true.
     *
     * @param ordered true if the writes should be ordered
     * @return this
     */
    public BulkWriteOptions ordered(final boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    /**
     * Gets the bypass document level validation flag
     *
     * @return the bypass document level validation flag
     * @since 3.2
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
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public BulkWriteOptions bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    /**
     * Returns the comment to send with the query. The default is not to include a comment with the query.
     *
     * @return the comment
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    @Nullable
    public BsonValue getComment() {
        return comment;
    }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    public BulkWriteOptions comment(@Nullable final String comment) {
        this.comment = comment != null ? new BsonString(comment) : null;
        return this;
    }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    public BulkWriteOptions comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    /**
     * Add top-level variables to the operation
     *
     * @return the top level variables if set or null.
     * @mongodb.server.release 5.0
     * @since 4.6
     */
    @Nullable
    public Bson getLet() {
        return variables;
    }

    /**
     * Add top-level variables for the operation
     *
     * <p>Allows for improved command readability by separating the variables from the query text.
     * The value of let will be passed to all update and delete, but not insert, commands.
     *
     * @param variables for the operation or null
     * @return this
     * @mongodb.server.release 5.0
     * @since 4.6
     */
    public BulkWriteOptions let(@Nullable final Bson variables) {
        this.variables = variables;
        return this;
    }

    @Override
    public String toString() {
        return "BulkWriteOptions{"
                + "ordered=" + ordered
                + ", bypassDocumentValidation=" + bypassDocumentValidation
                + ", comment=" + comment
                + ", let=" + variables
                + '}';
    }
}
