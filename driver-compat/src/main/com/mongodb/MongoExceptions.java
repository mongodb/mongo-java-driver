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

import java.io.IOException;

import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.command.MongoWriteConcernException;

public class MongoExceptions {
    @SuppressWarnings("deprecation")
    public static com.mongodb.MongoException mapException(final org.mongodb.MongoException e) {
        final Throwable cause = e.getCause();
        if (e instanceof org.mongodb.command.MongoDuplicateKeyException) {
            return new MongoException.DuplicateKey((org.mongodb.command.MongoDuplicateKeyException) e);
        } else if (e instanceof MongoWriteConcernException) {
            return new WriteConcernException((MongoWriteConcernException) e);
        } else if (e instanceof org.mongodb.MongoInternalException) {
            return new MongoInternalException((org.mongodb.MongoInternalException) e);
        } else if (e instanceof org.mongodb.connection.MongoTimeoutException) {
            return new MongoTimeoutException(e.getMessage());
        } else if (e instanceof org.mongodb.connection.MongoWaitQueueFullException) {
            return new MongoWaitQueueFullException(e.getMessage());
        } else if (e instanceof org.mongodb.operation.MongoCursorNotFoundException) {
            return new MongoCursorNotFoundException((org.mongodb.operation.MongoCursorNotFoundException) e);
        } else if (e instanceof MongoCommandFailureException) {
            return new CommandFailureException((MongoCommandFailureException) e);
        } else if (e instanceof org.mongodb.MongoInterruptedException) {
            return new MongoInterruptedException((org.mongodb.MongoInterruptedException) e);
        } else if (e instanceof org.mongodb.connection.MongoSocketException && cause instanceof IOException) {
            return new MongoSocketException(e.getMessage(), (IOException) cause);
        }

        return new MongoException(e.getMessage(), cause);
    }
}
