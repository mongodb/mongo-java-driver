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
    private Bson hint;
    private String hintString;
    private BsonValue comment;
    private Bson variables;
    private Bson sort;

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
     * Gets the bypass document level validation flag
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
     * <p>For bulk operations use: {@link BulkWriteOptions#bypassDocumentValidation(Boolean)}</p>
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

    /**
     * Returns the hint for which index to use. The default is not to set a hint.
     *
     * @return the hint
     * @since 4.1
     */
    @Nullable
    public Bson getHint() {
        return hint;
    }

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the hint
     * @return this
     * @since 4.1
     */
    public ReplaceOptions hint(@Nullable final Bson hint) {
        this.hint = hint;
        return this;
    }

    /**
     * Gets the hint string to apply.
     *
     * @return the hint string, which should be the name of an existing index
     * @since 4.1
     */
    @Nullable
    public String getHintString() {
        return hintString;
    }

    /**
     * Sets the hint to apply.
     *
     * @param hint the name of the index which should be used for the operation
     * @return this
     * @since 4.1
     */
    public ReplaceOptions hintString(@Nullable final String hint) {
        this.hintString = hint;
        return this;
    }
    /**
     * @return the comment for this operation. A null value means no comment is set.
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
     * <p>For bulk operations use: {@link BulkWriteOptions#comment(String)}</p>
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    public ReplaceOptions comment(@Nullable final String comment) {
        this.comment = comment != null ? new BsonString(comment) : null;
        return this;
    }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * <p>For bulk operations use: {@link BulkWriteOptions#comment(BsonValue)}</p>
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    public ReplaceOptions comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }
    /**
     * Add top-level variables to the operation
     *
     * <p>The value of let will be passed to all update and delete, but not insert, commands.
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
     * <p>For bulk operations use: {@link BulkWriteOptions#let(Bson)}
     *
     * @param variables for the operation or null
     * @return this
     * @mongodb.server.release 5.0
     * @since 4.6
     */
    public ReplaceOptions let(final Bson variables) {
        this.variables = variables;
        return this;
    }

    /**
     * Gets the sort criteria to apply to the operation.
     *
     * <p>
     * The sort criteria determines which document the operation replaces if the query matches multiple documents.
     * The first document matched by the sort criteria will be replaced.
     * The default is null, which means no specific sort criteria is applied.
     *
     * @return a document describing the sort criteria, or null if no sort is specified.
     * @mongodb.driver.manual reference/method/db.collection.replaceOne/ Sort
     * @mongodb.server.release 8.0
     * @since 5.3
     * @see #sort(Bson)
     */
    @Nullable
    public Bson getSort() {
        return sort;
    }

    /**
     * Sets the sort criteria to apply to the operation. A null value means no sort criteria is set.
     *
     * <p>
     * The sort criteria determines which document the operation replaces if the query matches multiple documents.
     * The first document matched by the specified sort criteria will be replaced.
     *
     * @param sort the sort criteria, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.replaceOne/ Sort
     * @mongodb.server.release 8.0
     * @since 5.3
     */
    public ReplaceOptions sort(@Nullable final Bson sort) {
        this.sort = sort;
        return this;
    }

    @Override
    public String toString() {
        return "ReplaceOptions{"
                + "upsert=" + upsert
                + ", bypassDocumentValidation=" + bypassDocumentValidation
                + ", collation=" + collation
                + ", hint=" + hint
                + ", hintString=" + hintString
                + ", comment=" + comment
                + ", let=" + variables
                + ", sort=" + sort
                + '}';
    }
}
