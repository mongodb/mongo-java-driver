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

package com.mongodb.connection;

import org.bson.io.BsonOutput;

/**
 * Base class for OP_QUERY messages.
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 */
abstract class BaseQueryMessage extends LegacyMessage {
    private final int skip;
    private final int numberToReturn;
    private boolean tailableCursor;
    private boolean slaveOk;
    private boolean oplogReplay;
    private boolean noCursorTimeout;
    private boolean awaitData;
    private boolean partial;

    BaseQueryMessage(final String collectionName, final int skip, final int numberToReturn, final MessageSettings settings) {
        super(collectionName, OpCode.OP_QUERY, settings);
        this.skip = skip;
        this.numberToReturn = numberToReturn;
    }

    /**
     * Gets whether the cursor is configured to be a tailable cursor.
     *
     * <p>Tailable means the cursor is not closed when the last data is retrieved. Rather, the cursor marks the final object's position. You
     * can resume using the cursor later, from where it was located, if more data were received. Like any "latent cursor",
     * the cursor may become invalid at some point - for example if the final object it references were deleted.</p>
     *
     * @return true if the cursor is configured to be a tailable cursor
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isTailableCursor() {
        return tailableCursor;
    }

    /**
     * Sets whether the cursor should be a tailable cursor.
     *
     * <p>Tailable means the cursor is not closed when the last data is retrieved. Rather, the cursor marks the final object's position. You
     * can resume using the cursor later, from where it was located, if more data were received. Like any "latent cursor",
     * the cursor may become invalid at some point - for example if the final object it references were deleted.</p>
     *
     * @param tailableCursor whether the cursor should be a tailable cursor.
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public BaseQueryMessage tailableCursor(final boolean tailableCursor) {
        this.tailableCursor = tailableCursor;
        return this;
    }

    /**
     * Returns true if set to allowed to query non-primary replica set members.
     *
     * @return true if set to allowed to query non-primary replica set members.
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isSlaveOk() {
        return slaveOk;
    }

    /**
     * Sets if allowed to query non-primary replica set members.
     *
     * @param slaveOk true if allowed to query non-primary replica set members.
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public BaseQueryMessage slaveOk(final boolean slaveOk) {
        this.slaveOk = slaveOk;
        return this;
    }

    /**
     * Internal replication use only.  Driver users should ordinarily not use this.
     *
     * @return oplogReplay
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isOplogReplay() {
        return oplogReplay;
    }

    /**
     * Internal replication use only.  Driver users should ordinarily not use this.
     *
     * @param oplogReplay the oplogReplay value
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public BaseQueryMessage oplogReplay(final boolean oplogReplay) {
        this.oplogReplay = oplogReplay;
        return this;
    }

    /**
     * Returns true if cursor timeout has been turned off.
     *
     * <p>The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use.</p>
     *
     * @return if cursor timeout has been turned off
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isNoCursorTimeout() {
        return noCursorTimeout;
    }

    /**
     * Sets if the cursor timeout should be turned off.
     *
     * @param noCursorTimeout true if the cursor timeout should be turned off.
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public BaseQueryMessage noCursorTimeout(final boolean noCursorTimeout) {
        this.noCursorTimeout = noCursorTimeout;
        return this;
    }

    /**
     * Returns true if the cursor should await for data.
     *
     * <p>Use with {@link #tailableCursor}. If we are at the end of the data, block for a while rather than returning no data. After a
     * timeout period, we do return as normal.</p>
     *
     * @return if the cursor should await for data
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isAwaitData() {
        return awaitData;
    }

    /**
     * Sets if the cursor should await for data.
     *
     * <p>Use with {@link #tailableCursor}. If we are at the end of the data, block for a while rather than returning no data. After a
     * timeout period, we do return as normal.</p>
     *
     * @param awaitData if we should await for data
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public BaseQueryMessage awaitData(final boolean awaitData) {
        this.awaitData = awaitData;
        return this;
    }

    /**
     * Returns true if can get partial results from a mongos if some shards are down.
     *
     * @return if can get partial results from a mongos if some shards are down
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isPartial() {
        return partial;
    }

    /**
     * Sets if partial results from a mongos if some shards are down are allowed
     *
     * @param partial allow partial results from a mongos if some shards are down
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public BaseQueryMessage partial(final boolean partial) {
        this.partial = partial;
        return this;
    }

    private int getCursorFlag() {
        int cursorFlag = 0;
        if (isTailableCursor()) {
            cursorFlag |= 1 << 1;
        }
        if (isSlaveOk()){
            cursorFlag |= 1 << 2;
        }
        if (isOplogReplay()){
            cursorFlag |= 1 << 3;
        }
        if (isNoCursorTimeout()){
            cursorFlag |= 1 << 4;
        }
        if (isAwaitData()){
            cursorFlag |= 1 << 5;
        }
        if (isPartial()) {
            cursorFlag |= 1 << 7;
        }
        return cursorFlag;
    }

    /**
     * Write the query prologue to the given BSON output.
     *
     * @param bsonOutput the BSON output
     */
    protected void writeQueryPrologue(final BsonOutput bsonOutput) {
        bsonOutput.writeInt32(getCursorFlag());
        bsonOutput.writeCString(getCollectionName());
        bsonOutput.writeInt32(skip);
        bsonOutput.writeInt32(numberToReturn);
    }
}
