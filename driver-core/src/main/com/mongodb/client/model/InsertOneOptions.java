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

/**
 * The options to apply to an operation that inserts a single document into a collection.
 *
 * @since 3.2
 * @mongodb.server.release 3.2
 * @mongodb.driver.manual tutorial/insert-documents/ Insert Tutorial
 * @mongodb.driver.manual reference/command/insert/ Insert Command
 */
public final class InsertOneOptions {
    private Boolean bypassDocumentValidation;
    private BsonValue comment;

    /**
     * Gets the bypass document level validation flag
     *
     * @return the bypass document level validation flag
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
     */
    public InsertOneOptions bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
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
    public InsertOneOptions comment(@Nullable final String comment) {
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
    public InsertOneOptions comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public String toString() {
        return "InsertOneOptions{"
                + "bypassDocumentValidation=" + bypassDocumentValidation
                + ", comment=" + comment
                + '}';
    }
}
