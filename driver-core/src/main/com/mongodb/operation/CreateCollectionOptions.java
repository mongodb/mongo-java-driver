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

package com.mongodb.operation;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Options for creating a collection
 *
 * @since 3.0
 * @mongodb.driver.manual reference/method/db.createCollection/ Create Collection
 */
public class CreateCollectionOptions {
    private final String collectionName;
    private final boolean autoIndex;
    private final long maxDocuments;
    private final boolean capped;
    private final long sizeInBytes;
    private final Boolean usePowerOf2Sizes;

    /**
     * Construct a new instance.
     *
     * @param collectionName the collection name.
     */
    public CreateCollectionOptions(final String collectionName) {
        this(collectionName, false, 0, true);
    }

    /**
     * Construct a new instance.
     *
     * @param collectionName the collection name.
     * @param usePowerOf2Sizes use the usePowerOf2Sizes allocation strategy for this collection.
     *
     * @mongodb.driver.manual manual/reference/command/collMod/#usePowerOf2Sizes usePowerOf2Sizes
     * @mongodb.server.release 2.6
     */
    public CreateCollectionOptions(final String collectionName, final boolean usePowerOf2Sizes) {
        this(collectionName, false, 0, true, 0, usePowerOf2Sizes);
    }

    /**
     * Construct a new instance.
     *
     * @param collectionName the collection name.
     * @param capped whether the collection is capped.
     * @param sizeInBytes the maximum size of the collection in bytes.  Only applies to capped collections.
     */
    public CreateCollectionOptions(final String collectionName, final boolean capped, final long sizeInBytes) {
        this(collectionName, capped, sizeInBytes, true);
    }

    /**
     * Construct a new instance.
     *
     * @param collectionName the collection name.
     * @param capped whether the collection is capped.
     * @param sizeInBytes the maximum size of the collection in bytes.  Only applies to capped collections.
     * @param autoIndex whether the _id field of the collection is indexed.  Only applies to capped collections.
     */
    public CreateCollectionOptions(final String collectionName, final boolean capped, final long sizeInBytes, final boolean autoIndex) {
        this(collectionName, capped, sizeInBytes, autoIndex, 0);
    }

    /**
     * Construct a new instance.
     *
     * @param collectionName the collection name
     * @param capped whether the collection is capped
     * @param sizeInBytes the maximum size of the collection in bytes.  Only applies to capped collections.
     * @param autoIndex whether the _id field of the collection is indexed.  Only applies to capped collections
     * @param maxDocuments the maximum number of documents in the collection.  Only applies to capped collections
     */
    public CreateCollectionOptions(final String collectionName, final boolean capped, final long sizeInBytes, final boolean autoIndex,
                                   final long maxDocuments) {
        this(collectionName, capped, sizeInBytes, autoIndex, maxDocuments, null);
    }

    /**
     * Construct a new instance.
     *
     * @param collectionName the collection name
     * @param capped whether the collection is capped
     * @param sizeInBytes the maximum size of the collection in bytes.  Only applies to capped collections.
     * @param autoIndex whether the _id field of the collection is indexed.  Only applies to capped collections
     * @param maxDocuments the maximum number of documents in the collection.  Only applies to capped collections
     * @param usePowerOf2Sizes use the usePowerOf2Sizes allocation strategy for this collection.
     *
     * @mongodb.driver.manual manual/reference/command/collMod/#usePowerOf2Sizes usePowerOf2Sizes
     * @mongodb.server.release 2.6
     */
    public CreateCollectionOptions(final String collectionName, final boolean capped, final long sizeInBytes, final boolean autoIndex,
                                   final long maxDocuments, final Boolean usePowerOf2Sizes) {
        this.collectionName = notNull("collectionName", collectionName);
        this.capped = capped;
        this.sizeInBytes = sizeInBytes;
        this.autoIndex = autoIndex;
        this.maxDocuments = maxDocuments;
        this.usePowerOf2Sizes = usePowerOf2Sizes;
    }

    /**
     * Gets the name of the collection to create.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     *
     * @return true if auto-index is enabled
     */
    public boolean isAutoIndex() {
        return autoIndex;
    }

    /**
     * Gets the maximum number of documents allowed in the collection.
     *
     * @return max number of documents in the collection
     */
    public long getMaxDocuments() {
        return maxDocuments;
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
     * Gets the maximum size of the collection in bytes.
     *
     * @return the maximum size of the collection
     */
    public long getSizeInBytes() {
        return sizeInBytes;
    }

    /**
     * Gets whether the usePowerOf2Sizes allocation strategy is turned on for this collection.
     *
     * @return true if the usePowerOf2Sizes allocation strategy is turned on for this collection
     * @mongodb.driver.manual manual/reference/command/collMod/#usePowerOf2Sizes usePowerOf2Sizes
     * @mongodb.server.release 2.6
     */
    public Boolean isUsePowerOf2Sizes() {
        return usePowerOf2Sizes;
    }
}
