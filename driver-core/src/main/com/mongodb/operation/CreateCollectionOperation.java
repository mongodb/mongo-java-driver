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

import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.operation.OperationHelper.ignoreResult;

/**
 * An operation to create a collection
 *
 * @since 3.0
 */
public class CreateCollectionOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final String databaseName;
    private final String collectionName;
    private boolean capped = false;
    private long sizeInBytes = 0;
    private boolean autoIndex = true;
    private long maxDocuments = 0;
    private Boolean usePowerOf2Sizes = null;

    /**
     * Construct a new instance
     *
     * @param databaseName the database name to create the collection in.
     * @param collectionName the name of the collection to be created.
     */
    public CreateCollectionOperation(final String databaseName, final String collectionName) {
        this.databaseName = notNull("databaseName", databaseName);
        this.collectionName = notNull("collectionName", collectionName);
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
     * The auto index value.
     *
     * @return true if auto-index is enabled
     */
    public boolean isAutoIndex() {
        return autoIndex;
    }

    /**
     * Sets if _id field of the collection is indexed.  Only applies to capped collections and defaults to true.
     *
     * @param autoIndex true if auto-index of _id is enabled. Only applies to capped collections.
     * @return this
     */
    public CreateCollectionOperation autoIndex(final boolean autoIndex) {
        this.autoIndex = autoIndex;
        return this;
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
     * Set the maximum number of documents in the collection.  Only applies to capped collections
     *
     * @param maxDocuments the maximum number of documents in the collection.  Only applies to capped collections.
     * @return this
     */
    public CreateCollectionOperation maxDocuments(final long maxDocuments) {
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
     * Sets whether the collection is capped.
     *
     * Capped collections also require the size set see {@link #sizeInBytes }.
     *
     * @param capped whether the collection is capped. Defaults to false.
     * @return this
     */
    public CreateCollectionOperation capped(final boolean capped) {
        this.capped = capped;
        return this;
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
     * Sets the maximum size of the collection in bytes. Required for capped collections.
     *
     * @param sizeInBytes the maximum size of the collection
     * @return this
     */
    public CreateCollectionOperation sizeInBytes(final long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
        return this;
    }

    /**
     * Gets whether usePowerOf2Sizes should be used foe the allocation strategy.
     *
     * <p>Note: {@code }usePowerOf2Sizes} became the default allocation strategy in mongodb 2.6</p>
     *
     * @return  usePowerOf2Sizes became the default allocation strategy
     * @mongodb.driver.manual manual/reference/command/collMod/#usePowerOf2Sizes usePowerOf2Sizes
     * @mongodb.server.release 2.6
     */
    public Boolean isUsePowerOf2Sizes() {
        return usePowerOf2Sizes;
    }

    /**
     * Sets whether usePowerOf2Sizes should be used foe the allocation strategy.
     *
     * <p>Note: {@code }usePowerOf2Sizes} became the default allocation strategy in mongodb 2.6</p>
     *
     * @param usePowerOf2Sizes as the default allocation strategy
     * @return this
     * @mongodb.driver.manual manual/reference/command/collMod/#usePowerOf2Sizes usePowerOf2Sizes
     * @mongodb.server.release 2.6
     */
    public CreateCollectionOperation setUsePowerOf2Sizes(final Boolean usePowerOf2Sizes) {
        this.usePowerOf2Sizes = usePowerOf2Sizes;
        return this;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        executeWrappedCommandProtocol(databaseName, asDocument(), new BsonDocumentCodec(), binding);
        return null;
    }

    @Override
    public MongoFuture<Void> executeAsync(final AsyncWriteBinding binding) {
        return ignoreResult(executeWrappedCommandProtocolAsync(databaseName, asDocument(), new BsonDocumentCodec(), binding));
    }

    private BsonDocument asDocument() {
        BsonDocument document = new BsonDocument("create", new BsonString(getCollectionName()));
        document.put("capped", BsonBoolean.valueOf(isCapped()));
        if (isCapped()) {
            putIfNotZero(document, "size", getSizeInBytes());
            document.put("autoIndexId", BsonBoolean.valueOf(isAutoIndex()));
            putIfNotZero(document, "max", getMaxDocuments());
        }
        if (isUsePowerOf2Sizes() != null) {
            document.put("usePowerOfTwoSizes", BsonBoolean.valueOf(usePowerOf2Sizes));
        }
        return document;
    }

}
