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
 *
 */

package com.mongodb.client.model;

import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Options for the $out aggregation stage
 *
 * @since 3.11
 * @see Aggregates#out(String, AggregateOutStageOptions)
 * @mongodb.driver.manual reference/operator/aggregation/out/  $out
 */
public class AggregateOutStageOptions {

    /**
     * The output mode
     */
    public enum Mode {
        /**
         * Replace the collection.  Collection options and indexes are recreated on the output collection.  This is the default behavior.
         */
        REPLACE_COLLECTION,

        /**
         * Replace all the documents in the collection. Existing documents are overwritten with a replacement-style update (upsert: true,
         * multi: false). The query used for the update will be the extracted unique key.
         *
         * @see #getUniqueKey()
         * @mongodb.server.release 4.2
         */
        REPLACE_DOCUMENTS,

        /**
         * Insert new documents into the collection.  If a document exists with the same unique key, execution stops and an error is raised.

         * @see #getUniqueKey()
         * @mongodb.server.release 4.2
         */
        INSERT_DOCUMENTS
    }

    private Mode mode = Mode.REPLACE_COLLECTION;
    private String databaseName;
    private Bson uniqueKey;

    /**
     * Gets the output mode.
     *
     * @return the mode, which may not be null
     */
    public Mode getMode() {
        return mode;
    }

    /**
     * Sets the output mode.
     *
     * @param mode the mode, which may not be null
     * @return this
     */
    public AggregateOutStageOptions mode(final Mode mode) {
        notNull("mode", mode);
        this.mode = mode;
        return this;
    }

    /**
     * Gets the name of the database in which to write the output collection. The default behavior is to write the output collection in the
     * same database as the source collection.
     *
     * @return the database name, which may be null
     * @mongodb.server.release 4.2
     */
    @Nullable
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the name of the database in which to write the output collection.
     *
     * @param databaseName the database name, which may be null
     * @return this
     * @mongodb.server.release 4.2
     */
    public AggregateOutStageOptions databaseName(@Nullable final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    /**
     * Gets the document describing the unique keys used for comparing documents in the output collection when the mode is either
     * {@code Mode#REPLACE_DOCUMENTS} or {@code Mode#INSERT_DOCUMENTS}.  The format is similar to that used to define a unique index on one
     * or more fields, though the order of fields does not matter.  The default value is the document key:
     * <ul>
     *     <li>For an unsharded output collection, the document key is {@code {_id: 1}}.</li>
     *     <li>For a sharded output collection, the document key is {@code {_id: 1}} plus all the fields of the shard key.</li>
     * </ul>
     *
     * @return the unique key, which may be null
     * @see Mode#REPLACE_DOCUMENTS
     * @see Mode#INSERT_DOCUMENTS
     * @see Indexes#ascending(String...)
     * @mongodb.server.release 4.2
     */
    @Nullable
    public Bson getUniqueKey() {
        return uniqueKey;
    }

    /**
     * Sets the document describing the unique keys used for comparing documents in the output collection when the mode is either
     * {@code Mode#REPLACE_DOCUMENTS} or {@code Mode#INSERT_DOCUMENTS}.  The format is similar to that used to define a unique index on one
     * or more fields, though the order of fields does not matter.  The default value is the document key:
     * <ul>
     *     <li>For an unsharded output collection, the document key is {@code {_id: 1}}.</li>
     *     <li>For a sharded output collection, the document key is {@code {_id: 1}} plus all the fields of the shard key.</li>
     * </ul>
     *
     * @param uniqueKey the unique key, which may be null
     * @return this
     * @mongodb.server.release 4.2
     */
    public AggregateOutStageOptions uniqueKey(@Nullable final Bson uniqueKey) {
        this.uniqueKey = uniqueKey;
        return this;
    }
}
