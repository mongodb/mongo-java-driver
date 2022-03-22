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
 * The options to apply when deleting documents.
 *
 * @since 3.4
 * @mongodb.driver.manual tutorial/remove-documents/ Remove documents
 * @mongodb.driver.manual reference/command/delete/ Delete Command
 */
public class DeleteOptions {
    private Bson hint;
    private String hintString;
    private Collation collation;
    private BsonValue comment;

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
    public DeleteOptions collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Gets the hint to apply.
     *
     * @return the hint, which should describe an existing index
     * @since 4.1
     * @mongodb.server.release 4.4
     */
    @Nullable
    public Bson getHint() {
        return hint;
    }

    /**
     * Gets the hint string to apply.
     *
     * @return the hint string, which should be the name of an existing index
     * @since 4.1
     * @mongodb.server.release 4.4
     */
    @Nullable
    public String getHintString() {
        return hintString;
    }

    /**
     * Sets the hint to apply.
     *
     * @param hint a document describing the index which should be used for this operation.
     * @return this
     * @since 4.1
     * @mongodb.server.release 4.4
     */
    public DeleteOptions hint(@Nullable final Bson hint) {
        this.hint = hint;
        return this;
    }

    /**
     * Sets the hint to apply.
     *
     * <p>Note: If {@link DeleteOptions#hint(Bson)} is set that will be used instead of any hint string.</p>
     *
     * @param hint the name of the index which should be used for the operation
     * @return this
     * @since 4.1
     * @mongodb.server.release 4.4
     */
    public DeleteOptions hintString(@Nullable final String hint) {
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
    public DeleteOptions comment(@Nullable final String comment) {
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
    public DeleteOptions comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public String toString() {
        return "DeleteOptions{"
                + "collation=" + collation
                + ", hint=" + hint
                + ", hintString='" + hintString + '\''
                + ", comment=" + comment
                + '}';
    }
}

