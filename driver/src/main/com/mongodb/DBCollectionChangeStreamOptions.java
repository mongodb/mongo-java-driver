/*
 * Copyright 2017 MongoDB, Inc.
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

package com.mongodb;

import com.mongodb.client.model.Collation;
import com.mongodb.client.model.FullDocument;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The options to apply to a change stream operation.
 *
 * @since 3.6
 */
public final class DBCollectionChangeStreamOptions {
    private Integer batchSize;
    private Collation collation;
    private FullDocument fullDocument = notNull("FullDocument.DEFAULT", FullDocument.DEFAULT);
    private long maxAwaitTimeMS;
    private ReadConcern readConcern;
    private ReadPreference readPreference;
    private DBObject resumeToken;

    /**
     * Construct a new instance.
     */
    public DBCollectionChangeStreamOptions() {
    }

    /**
     * Returns the batchSize
     *
     * @return the batchSize
     */
    public Integer getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the batchSize
     *
     * @param batchSize the batchSize
     * @return this
     */
    public DBCollectionChangeStreamOptions batchSize(final Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Returns the collation
     *
     * @return the collation
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation
     *
     * @param collation the collation
     * @return this
     */
    public DBCollectionChangeStreamOptions collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Returns the fullDocument
     *
     * @return the fullDocument
     */
    public FullDocument getFullDocument() {
        return fullDocument;
    }

    /**
     * Sets the fullDocument
     *
     * @param fullDocument the fullDocument
     * @return this
     */
    public DBCollectionChangeStreamOptions fullDocument(final FullDocument fullDocument) {
        this.fullDocument = notNull("fullDocument", fullDocument);
        return this;
    }

    /**
     * The maximum amount of time for the server to wait on new documents to satisfy a tailable cursor
     * query. This only applies to a TAILABLE_AWAIT cursor. When the cursor is not a TAILABLE_AWAIT cursor,
     * this option is ignored.
     * <p>
     * A zero value will be ignored.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum await execution time in the given time unit
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public long getMaxAwaitTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxAwaitTimeMS, MILLISECONDS);
    }

    /**
     * Sets the maximum await execution time on the server for this operation.
     *
     * @param maxAwaitTime  the max await time.  A zero value will be ignored, and indicates that the driver should respect the server's
     *                      default value
     * @param timeUnit the time unit, which may not be null
     * @return this
     */
    public DBCollectionChangeStreamOptions maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxAwaitTimeMS = MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    /**
     * Gets the read concern
     *
     * @return the read concern
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * Sets the read concern
     *
     * @param readConcern the read concern
     * @return this
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    public DBCollectionChangeStreamOptions readConcern(final ReadConcern readConcern) {
        this.readConcern = readConcern;
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
    public DBCollectionChangeStreamOptions readPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
        return this;
    }

    /**
     * Returns the resumeToken
     *
     * @return the resumeToken
     */
    public DBObject getResumeToken() {
        return resumeToken;
    }

    /**
     * Sets the resumeToken
     *
     * @param resumeToken the resumeToken
     * @return this
     */
    public DBCollectionChangeStreamOptions resumeToken(final DBObject resumeToken) {
        this.resumeToken = resumeToken;
        return this;
    }
}
