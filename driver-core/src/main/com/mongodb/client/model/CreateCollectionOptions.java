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
     * Gets the storage engine options document for this index.
     *
     * @return the storage engine options
     * @mongodb.server.release 3.0
     */
    public Bson getStorageEngineOptions() {
        return storageEngineOptions;
    }

    /**
     * Sets the storage engine options document for this index.
     *
     * @param storageEngineOptions the storate engine options
     * @return this
     * @mongodb.server.release 3.0
     */
    public CreateCollectionOptions storageEngineOptions(final Bson storageEngineOptions) {
        this.storageEngineOptions = storageEngineOptions;
        return this;
    }
}
