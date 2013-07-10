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

import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.command.MongoDuplicateKeyException;
import org.mongodb.command.MongoWriteConcernException;
import org.mongodb.connection.MongoSocketException;
import org.mongodb.connection.MongoTimeoutException;
import org.mongodb.connection.MongoWaitQueueFullException;
import org.mongodb.operation.MongoCursorNotFoundException;
import org.mongodb.operation.ServerCursor;

import java.io.IOException;

public class MongoExceptions {
    public static com.mongodb.MongoException mapException(final org.mongodb.MongoException e) {
        final Throwable cause = e.getCause();
        if (e instanceof MongoDuplicateKeyException) {
            return new MongoException.DuplicateKey((MongoDuplicateKeyException) e);
        } else if (e instanceof MongoWriteConcernException) {
            return new WriteConcernException((MongoWriteConcernException) e);
        } else if (e instanceof org.mongodb.MongoInternalException) {
            return new MongoInternalException((org.mongodb.MongoInternalException) e);
        } else if (e instanceof MongoTimeoutException) {
            return new ConnectionWaitTimeOut(e.getMessage());
        } else if (e instanceof MongoWaitQueueFullException) {
            return new SemaphoresOut(e.getMessage());
        } else if (e instanceof MongoCursorNotFoundException) {
            final ServerCursor serverCursor = ((MongoCursorNotFoundException) e).getCursor();
            return new MongoException.CursorNotFound((MongoCursorNotFoundException) e);
        } else if (e instanceof MongoCommandFailureException) {
            return new CommandFailureException((MongoCommandFailureException) e);
        } else if (e instanceof org.mongodb.MongoInterruptedException) {
            return new MongoInterruptedException((org.mongodb.MongoInterruptedException) e);
        } else if (e instanceof MongoSocketException && cause instanceof IOException) {
            return new MongoException.Network(e.getMessage(), (IOException) cause);
        }

        return new MongoException(e.getMessage(), cause);
    }
}
