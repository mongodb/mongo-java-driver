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

import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Options for creating a collection
 *
 * @mongodb.driver.manual reference/method/db.createCollection/ Create Collection
 * @since 3.0
 */
public class CreateCollectionOptions {
    private boolean autoIndex = true;
    private long maxDocuments;
    private boolean capped;
    private long sizeInBytes;
    private Boolean usePowerOf2Sizes;
    private Bson storageEngineOptions;
    private IndexOptionDefaults indexOptionDefaults = new IndexOptionDefaults();
    private ValidationOptions validationOptions = new ValidationOptions();
    private Collation collation;

    /**
     * Gets if auto-index is enabled
     *
     * @return true if auto-index is enabled
     */
    public boolean isAutoIndex() {
        return autoIndex;
    }

    /**
     * Gets if auto-index is to be enabled on the collection
     *
     * @param autoIndex true if auto-index is enabled
     * @return this
     */
    public CreateCollectionOptions autoIndex(final boolean autoIndex) {
        this.autoIndex = autoIndex;
        return this;
    }

    /**
     * Gets the maximum number of documents allowed in a capped collection.
     *
     * @return max number of documents in a capped collection
     */
    public long getMaxDocuments() {
        return maxDocuments;
    }

    /**
     * Sets the maximum number of documents allowed in a capped collection.
     *
     * @param maxDocuments the maximum number of documents allowed in capped collection
     * @return this
     */
    public CreateCollectionOptions maxDocuments(final long maxDocuments) {
        this.maxDocuments = maxDocuments;
        return this;
    }

    /**
     * Gets whether the collection is capped.
     *
     * @return whether the collection is capped
     */
    public boolean isCapped() {
        return capped;
    }


    /**
     * sets whether the collection is capped.
     *
     * @param capped whether the collection is capped
     * @return this
     */
    public CreateCollectionOptions capped(final boolean capped) {
        this.capped = capped;
        return this;
    }

    /**
     * Gets the maximum size in bytes of a capped collection.
     *
     * @return the maximum size of a capped collection.
     */
    public long getSizeInBytes() {
        return sizeInBytes;
    }

    /**
     * Gets the maximum size of in bytes of a capped collection.
     *
     * @param sizeInBytes the maximum size of a capped collection.
     * @return this
     */
    public CreateCollectionOptions sizeInBytes(final long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
        return this;
    }

    /**
     * Gets whether the usePowerOf2Sizes allocation strategy is turned on for this collection.
     *
     * @return true if the usePowerOf2Sizes allocation strategy is turned on for this collection
     * @mongodb.driver.manual reference/command/collMod/#usePowerOf2Sizes usePowerOf2Sizes
     * @mongodb.server.release 2.6
     */
    public Boolean isUsePowerOf2Sizes() {
        return usePowerOf2Sizes;
    }

    /**
     * Sets whether the usePowerOf2Sizes allocation strategy is turned on for this collection.
     *
     * @param usePowerOf2Sizes true if the usePowerOf2Sizes allocation strategy is turned on for this collection
     * @return this
     * @mongodb.driver.manual reference/command/collMod/#usePowerOf2Sizes usePowerOf2Sizes
     * @mongodb.server.release 2.6
     */
    public CreateCollectionOptions usePowerOf2Sizes(final Boolean usePowerOf2Sizes) {
        this.usePowerOf2Sizes = usePowerOf2Sizes;
        return this;
    }

    /**
     * Gets the storage engine options document for the collection.
     *
     * @return the storage engine options
     * @mongodb.server.release 3.0
     */
    public Bson getStorageEngineOptions() {
        return storageEngineOptions;
    }

    /**
     * Sets the storage engine options document defaults for the collection
     *
     * @param storageEngineOptions the storage engine options
     * @return this
     * @mongodb.server.release 3.0
     */
    public CreateCollectionOptions storageEngineOptions(final Bson storageEngineOptions) {
        this.storageEngineOptions = storageEngineOptions;
        return this;
    }

    /**
     * Gets the index option defaults for the collection.
     *
     * @return the index option defaults
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public IndexOptionDefaults getIndexOptionDefaults() {
        return indexOptionDefaults;
    }

    /**
     * Sets the index option defaults for the collection.
     *
     * @param indexOptionDefaults the index option defaults
     * @return this
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public CreateCollectionOptions indexOptionDefaults(final IndexOptionDefaults indexOptionDefaults) {
        this.indexOptionDefaults = indexOptionDefaults;
        return this;
    }

    /**
     * Gets the validation options for documents being inserted or updated in a collection
     *
     * @return the validation options
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public ValidationOptions getValidationOptions() {
        return validationOptions;
    }

    /**
     * Sets the validation options for documents being inserted or updated in a collection
     *
     * @param validationOptions the validation options
     * @return this
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public CreateCollectionOptions validationOptions(final ValidationOptions validationOptions) {
        this.validationOptions = notNull("validationOptions", validationOptions);
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public CreateCollectionOptions collation(final Collation collation) {
        this.collation = collation;
        return this;
    }
}
