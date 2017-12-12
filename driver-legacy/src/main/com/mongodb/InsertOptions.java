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

package com.mongodb;

/**
 * Options related to insertion of documents into MongoDB.  The setter methods return {@code this} so that a chaining style can be used.
 *
 * @since 2.13
 * @mongodb.driver.manual tutorial/insert-documents/ Insert Tutorial
 */
public final class InsertOptions {
    private WriteConcern writeConcern;
    private boolean continueOnError;
    private DBEncoder dbEncoder;
    private Boolean bypassDocumentValidation;

    /**
     * Set the write concern to use for the insert.
     *
     * @param writeConcern the write concern
     * @return this
     */
    public InsertOptions writeConcern(final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
        return this;
    }

    /**
     * Set whether documents will continue to be inserted after a failure to insert one.
     *
     * @param continueOnError whether to continue on error
     * @return this
     */
    public InsertOptions continueOnError(final boolean continueOnError) {
        this.continueOnError = continueOnError;
        return this;
    }

    /**
     * Set the encoder to use for the documents.
     *
     * @param dbEncoder the encoder
     * @return this
     */
    public InsertOptions dbEncoder(final DBEncoder dbEncoder) {
        this.dbEncoder = dbEncoder;
        return this;
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
     * Whether documents will continue to be inserted after a failure to insert one (most commonly due to a duplicate key error).  Note that
     * this only is relevant for multi-document inserts. The default value is false.
     *
     * @return whether insertion will continue on error.
     */
    public boolean isContinueOnError() {
        return continueOnError;
    }

    /**
     * The encoder to use for the documents.  By default the codec configured for the DBCollection instance will be used.
     *
     * @return the encoder, or null if the default will be used
     */
    public DBEncoder getDbEncoder() {
        return dbEncoder;
    }


    /**
     * Gets whether to bypass document validation, or null if unspecified.  The default is null.
     *
     * @return whether to bypass document validation, or null if unspecified.
     * @since 2.14
     * @mongodb.server.release 3.2
     */
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * Sets whether to bypass document validation.
     *
     * @param bypassDocumentValidation whether to bypass document validation, or null if unspecified
     * @return this
     * @since 2.14
     * @mongodb.server.release 3.2
     */
    public InsertOptions bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }
}
