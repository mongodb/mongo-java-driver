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

package com.mongodb;

import java.util.List;

/**
 * The result of a successful bulk write operation.
 *
 * @mongodb.server.release 2.6
 * @mongodb.driver.manual reference/command/delete/#delete-command-output Delete Result
 * @mongodb.driver.manual reference/command/update/#delete-command-output Delete Result
 * @mongodb.driver.manual reference/command/insert/#delete-command-output Delete Result
 * @since 2.12
 */
public abstract class BulkWriteResult {

    /**
     * Returns true if the write was acknowledged.
     *
     * @return true if the write was acknowledged
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public abstract boolean isAcknowledged();

    /**
     * Returns the number of documents inserted by the write operation.
     *
     * @return the number of documents inserted by the write operation
     * @throws java.lang.UnsupportedOperationException if the write was unacknowledged.
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public abstract int getInsertedCount();

    /**
     * Returns the number of documents matched by updates or replacements in the write operation.  This will include documents that matched
     * the query but where the modification didn't result in any actual change to the document; for example, if you set the value of some
     * field, and the field already has that value, that will still count as an update.
     *
     * @return the number of documents matched by updates in the write operation
     * @throws java.lang.UnsupportedOperationException if the write was unacknowledged.
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public abstract int getMatchedCount();

    /**
     * Returns the number of documents removed by the write operation.
     *
     * @return the number of documents removed by the write operation
     * @throws java.lang.UnsupportedOperationException if the write was unacknowledged.
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public abstract int getRemovedCount();

    /**
     * Returns true if the server was able to provide a count of modified documents.  If this method returns false (which can happen if the
     * server is not at least version 2.6) then the {@code getModifiedCount} method will throw {@code UnsupportedOperationException}.
     *
     * @return true if modifiedCount is available
     * @throws java.lang.UnsupportedOperationException if the write was unacknowledged.
     * @see WriteConcern#UNACKNOWLEDGED
     * @see #getModifiedCount()
     */
    public abstract boolean isModifiedCountAvailable();

    /**
     * <p>Returns the number of documents modified by the write operation.  This only applies to updates or replacements, and will only
     * count documents that were actually changed; for example, if you set the value of some field , and the field already has that value,
     * that will not count as a modification.</p>
     *
     * <p>If the server is not able to provide a count of modified documents (which can happen if the server is not at least version 2.6),
     * then this method will throw an {@code UnsupportedOperationException} </p>
     *
     * @return the number of documents modified by the write operation
     * @throws java.lang.UnsupportedOperationException if the write was unacknowledged or if no modified count is available
     * @see WriteConcern#UNACKNOWLEDGED
     * @see #isModifiedCountAvailable()
     */
    public abstract int getModifiedCount();

    /**
     * Gets an unmodifiable list of upserted items, or the empty list if there were none.
     *
     * @return a list of upserted items, or the empty list if there were none.
     * @throws java.lang.UnsupportedOperationException if the write was unacknowledged.
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public abstract List<BulkWriteUpsert> getUpserts();
}
