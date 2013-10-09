/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
 * This class lets you access the results of the previous write. If the write was performed with an acknowledged write concern, this just
 * stores the result of the write.
 *
 * @see WriteConcern#UNACKNOWLEDGED
 */
public class WriteResult {

    private final com.mongodb.WriteConcern writeConcern;
    private final CommandResult commandResult;

    WriteResult(final CommandResult commandResult, final WriteConcern writeConcern) {
        this.commandResult = commandResult;
        this.writeConcern = writeConcern;
    }

    /**
     * Gets the result of the write operation.  If the write was not an acknowledged one, throws {@code MongoException}.
     *
     * @return the result of the write operation
     * @throws MongoException if the write was unacknowledged
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public com.mongodb.CommandResult getLastError() {
        if (commandResult == null) {
            throw new MongoException("Write was unacknowledged, so no result is available");
        }
        return commandResult;
    }

    /**
     * Gets the result of the write operation. If the write was not an acknowledged one, returns null.
     *
     * @return the result of the write operation, or null if the write was unacknowledged.
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public CommandResult getCachedLastError() {
        return commandResult;
    }

    /**
     * Gets the last {@code WriteConcern} used for the write operation.
     *
     * @return the write concern
     */
    public WriteConcern getLastConcern() {
        return writeConcern;
    }

    /**
     * Gets the error string for the write operation result (the "err" field)
     *
     * @return the error string
     * @throws MongoException if the write was unacknowledged
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public String getError() {
        Object errField = getField("err");
        if (errField == null) {
            return null;
        }
        return errField.toString();
    }

    /**
     * Gets the "n" field, which contains the number of documents affected in the write operation.
     *
     * @return the value of the "n" field
     * @throws MongoException if the write was unacknowledged
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public int getN() {
        return getLastError().getInt("n");
    }

    /**
     * Gets a field from the result document.
     *
     * @param name field name
     * @return the value of the field
     * @throws MongoException if the write was unacknowledged
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public Object getField(final String name) {
        return getLastError().get(name);
    }

    /**
     * Returns whether or not the result is lazy, meaning that getLastError was not called automatically
     *
     * @return true if the write operation was unacknowledged
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public boolean isLazy() {
        return commandResult == null;
    }

    @Override
    public String toString() {
        return "WriteResult{"
               + "writeConcern=" + writeConcern
               + ", commandResult=" + commandResult
               + '}';
    }
}
