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

import com.mongodb.DBEncoder;
import com.mongodb.WriteConcern;
import com.mongodb.lang.Nullable;

/**
 * The options to apply when removing documents from the DBCollection
 *
 * @since 3.4
 * @mongodb.driver.manual tutorial/remove-documents/ Remove Documents
 */
public final class DBCollectionRemoveOptions {
    private Collation collation;
    private WriteConcern writeConcern;
    private DBEncoder encoder;

    /**
     * Construct a new instance
     */
    public DBCollectionRemoveOptions() {
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
     * Sets the collation
     *
     * @param collation the collation
     * @return this
     * @mongodb.server.release 3.4
     */
    public DBCollectionRemoveOptions collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * The write concern to use for the insertion.  By default the write concern configured for the DBCollection instance will be used.
     *
     * @return the write concern, or null if the default will be used.
     */
    @Nullable
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Sets the write concern
     *
     * @param writeConcern the write concern
     * @return this
     */
    public DBCollectionRemoveOptions writeConcern(@Nullable final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
        return this;
    }

    /**
     * Returns the encoder
     *
     * @return the encoder
     */
    @Nullable
    public DBEncoder getEncoder() {
        return encoder;
    }

    /**
     * Sets the encoder
     *
     * @param encoder the encoder
     * @return this
     */
    public DBCollectionRemoveOptions encoder(@Nullable final DBEncoder encoder) {
        this.encoder = encoder;
        return this;
    }
}
