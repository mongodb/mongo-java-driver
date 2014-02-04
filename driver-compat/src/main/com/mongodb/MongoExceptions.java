/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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

import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoDuplicateKeyException;
import org.mongodb.MongoWriteException;

import java.io.IOException;

final class MongoExceptions {
    @SuppressWarnings("deprecation")
    public static com.mongodb.MongoException mapException(final org.mongodb.MongoException e) {
        Throwable cause = e.getCause();
        if (e instanceof org.mongodb.MongoDuplicateKeyException) {
            return new MongoException.DuplicateKey((MongoDuplicateKeyException) e);
        } else if (e instanceof org.mongodb.MongoIncompatibleDriverException) {
            return new MongoIncompatibleDriverException(e.getMessage());
        } else if (e instanceof org.mongodb.MongoExecutionTimeoutException) {
            return new MongoExecutionTimeoutException((org.mongodb.MongoExecutionTimeoutException) e);
        } else if (e instanceof MongoWriteException) {
            return new WriteConcernException((MongoWriteException) e);
        } else if (e instanceof org.mongodb.MongoInternalException) {
            return new MongoInternalException((org.mongodb.MongoInternalException) e);
        } else if (e instanceof org.mongodb.connection.MongoTimeoutException) {
            return new MongoTimeoutException(e.getMessage());
        } else if (e instanceof org.mongodb.connection.MongoWaitQueueFullException) {
            return new MongoWaitQueueFullException(e.getMessage());
        } else if (e instanceof org.mongodb.MongoCursorNotFoundException) {
            return new MongoCursorNotFoundException((org.mongodb.MongoCursorNotFoundException) e);
        } else if (e instanceof MongoCommandFailureException) {
            return new CommandFailureException((MongoCommandFailureException) e);
        } else if (e instanceof org.mongodb.MongoInterruptedException) {
            return new MongoInterruptedException((org.mongodb.MongoInterruptedException) e);
        } else if (e instanceof org.mongodb.connection.MongoSocketException && cause instanceof IOException) {
            return new MongoSocketException(e.getMessage(), (IOException) cause);
        } else if (e instanceof org.mongodb.BulkWriteException) {
            return BulkWriteHelper.translateBulkWriteException((org.mongodb.BulkWriteException) e);
        } else if (e instanceof org.mongodb.connection.MongoServerSelectionException) {
            return new MongoServerSelectionException(e.getMessage());
        } else {
            return new MongoException(e.getMessage(), cause);
        }
    }

    private MongoExceptions() {
    }
}
