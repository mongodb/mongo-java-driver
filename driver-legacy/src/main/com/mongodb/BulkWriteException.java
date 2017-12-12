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

import java.util.List;

/**
 * An exception that represents all errors associated with a bulk write operation.
 *
 * @mongodb.server.release 2.6
 * @mongodb.driver.manual reference/method/BulkWriteResult/#BulkWriteResult.writeErrors BulkWriteResult.writeErrors
 * @since 2.12
 */
public class BulkWriteException extends MongoServerException {
    private static final long serialVersionUID = -1505950263354313025L;

    private final BulkWriteResult writeResult;
    private final List<BulkWriteError> writeErrors;
    private final ServerAddress serverAddress;
    private final WriteConcernError writeConcernError;

    /**
     * Constructs a new instance.
     *
     * @param writeResult              the write result
     * @param writeErrors              the list of write errors
     * @param writeConcernError        the write concern error
     * @param serverAddress            the server address.
     */
    BulkWriteException(final BulkWriteResult writeResult, final List<BulkWriteError> writeErrors,
                       final WriteConcernError writeConcernError, final ServerAddress serverAddress) {
        super("Bulk write operation error on server " + serverAddress + ". "
              + (writeErrors.isEmpty() ? "" : "Write errors: " + writeErrors + ". ")
              + (writeConcernError == null ? "" : "Write concern error: " + writeConcernError + ". "), serverAddress);
        this.writeResult = writeResult;
        this.writeErrors = writeErrors;
        this.writeConcernError = writeConcernError;
        this.serverAddress = serverAddress;
    }

    /**
     * The result of all successfully processed write operations.  This will never be null.
     *
     * @return the bulk write result
     */
    public BulkWriteResult getWriteResult() {
        return writeResult;
    }

    /**
     * The list of errors, which will not be null, but may be empty (if the write concern error is not null).
     *
     * @return the list of errors
     */
    public List<BulkWriteError> getWriteErrors() {
        return writeErrors;
    }

    /**
     * The write concern error, which may be null (in which case the list of errors will not be empty).
     *
     * @return the write concern error
     */
    public WriteConcernError getWriteConcernError() {
        return writeConcernError;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BulkWriteException that = (BulkWriteException) o;

        if (!writeErrors.equals(that.writeErrors)) {
            return false;
        }
        if (!serverAddress.equals(that.serverAddress)) {
            return false;
        }
        if (writeConcernError != null ? !writeConcernError.equals(that.writeConcernError) : that.writeConcernError != null) {
            return false;
        }
        if (!writeResult.equals(that.writeResult)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = writeResult.hashCode();
        result = 31 * result + writeErrors.hashCode();
        result = 31 * result + serverAddress.hashCode();
        result = 31 * result + (writeConcernError != null ? writeConcernError.hashCode() : 0);
        return result;
    }
}

