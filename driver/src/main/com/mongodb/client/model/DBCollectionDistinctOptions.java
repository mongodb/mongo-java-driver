/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model;

import com.mongodb.DBObject;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;

/**
 * The options for a count operation.
 *
 * @since 3.4
 * @mongodb.driver.manual reference/command/count/ Count
 */
public class DBCollectionDistinctOptions {
    private DBObject filter;
    private ReadPreference readPreference;
    private ReadConcern readConcern;
    private Collation collation;

    /**
     * Construct a new instance
     */
    public DBCollectionDistinctOptions() {
    }

    /**
     * Gets the selection query to determine the subset of documents from which to retrieve the distinct values
     *
     * @return the query
     */
    public DBObject getFilter() {
        return filter;
    }

    /**
     * Sets the selection query to determine the subset of documents from which to retrieve the distinct values.
     *
     * @param filter the selection query to determine the subset of documents from which to retrieve the distinct values
     * @return this
     */
    public DBCollectionDistinctOptions filter(final DBObject filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Returns the readPreference
     *
     * @return the readPreference
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    /**
     * Sets the readPreference
     *
     * @param readPreference the readPreference
     * @return this
     */
    public DBCollectionDistinctOptions readPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
        return this;
    }

    /**
     * Returns the readConcern
     *
     * @return the readConcern
     * @mongodb.server.release 3.2
     */
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * Sets the readConcern
     *
     * @param readConcern the readConcern
     * @return this
     * @mongodb.server.release 3.2
     */
    public DBCollectionDistinctOptions readConcern(final ReadConcern readConcern) {
        this.readConcern = readConcern;
        return this;
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
    public DBCollectionDistinctOptions collation(final Collation collation) {
        this.collation = collation;
        return this;
    }
}

