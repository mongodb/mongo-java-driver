/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.DBEncoder;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

import java.util.List;

/**
 * The options to apply when updating documents in the DBCollection
 *
 * @since 3.4
 * @mongodb.driver.manual tutorial/modify-documents/ Updates
 * @mongodb.driver.manual reference/operator/update/ Update Operators
 * @mongodb.driver.manual reference/command/update/ Update Command
 */
public class DBCollectionUpdateOptions {
    private boolean upsert;
    private Boolean bypassDocumentValidation;
    private boolean multi;
    private Collation collation;
    private List<? extends DBObject> arrayFilters;
    private WriteConcern writeConcern;
    private DBEncoder encoder;

    /**
     * Construct a new instance
     */
    public DBCollectionUpdateOptions() {
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
     * @param isUpsert true if a new document should be inserted if there are no matches to the query filter
     * @return this
     */
    public DBCollectionUpdateOptions upsert(final boolean isUpsert) {
        this.upsert = isUpsert;
        return this;
    }

    /**
     * Gets the the bypass document level validation flag
     *
     * @return the bypass document level validation flag
     * @mongodb.server.release 3.2
     */
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
    public DBCollectionUpdateOptions bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    /**
     * Sets whether all documents matching the query filter will be removed.
     *
     * @param multi true if all documents matching the query filter will be removed
     * @return this
     */
    public DBCollectionUpdateOptions multi(final boolean multi) {
        this.multi = multi;
        return this;
    }

    /**
     * Gets whether all documents matching the query filter will be removed.  The default is true.
     *
     * @return whether all documents matching the query filter will be removed
     */
    public boolean isMulti() {
        return multi;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @mongodb.server.release 3.4
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation
     *
     * @param collation the collation
     * @return this
     * @mongodb.server.release 3.4
     */
    public DBCollectionUpdateOptions collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Sets the array filters option
     *
     * @param arrayFilters the array filters, which may be null
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public DBCollectionUpdateOptions arrayFilters(final List<? extends DBObject> arrayFilters) {
        this.arrayFilters = arrayFilters;
        return this;
    }

    /**
     * Returns the array filters option
     *
     * @return the array filters, which may be null
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public List<? extends DBObject> getArrayFilters() {
        return arrayFilters;
    }

    /**
     * The write concern to use for the insertion.  By default the write concern configured for the DBCollection instance will be used.
     *
     * @return the write concern, or null if the default will be used.
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Sets the write concern
     *
     * @param writeConcern the write concern
     * @return this
     */
    public DBCollectionUpdateOptions writeConcern(final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
        return this;
    }

    /**
     * Returns the encoder
     *
     * @return the encoder
     */
    public DBEncoder getEncoder() {
        return encoder;
    }

    /**
     * Sets the encoder
     *
     * @param encoder the encoder
     * @return this
     */
    public DBCollectionUpdateOptions encoder(final DBEncoder encoder) {
        this.encoder = encoder;
        return this;
    }
}
