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

package com.mongodb.internal.operation;

import com.mongodb.MongoChangeStreamException;
import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoServerUnavailableException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.ServerAddress;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.mongodb.internal.operation.ChangeStreamBatchCursorHelper.RESUMABLE_CHANGE_STREAM_ERROR_LABEL;
import static com.mongodb.internal.operation.ChangeStreamBatchCursorHelper.RETRYABLE_SERVER_ERROR_CODES;
import static com.mongodb.internal.operation.ChangeStreamBatchCursorHelper.isResumableError;
import static com.mongodb.internal.operation.ServerVersionHelper.FOUR_DOT_FOUR_WIRE_VERSION;
import static com.mongodb.internal.operation.ServerVersionHelper.FOUR_DOT_TWO_WIRE_VERSION;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ChangeStreamBatchCursorHelperTest {
    @Test
    public void testIsResumableError() {
        assertFalse(isResumableError(new IllegalStateException(), FOUR_DOT_FOUR_WIRE_VERSION));
        assertFalse(isResumableError(new MongoChangeStreamException(""), FOUR_DOT_FOUR_WIRE_VERSION));
        assertFalse(isResumableError(new MongoInterruptedException("", new InterruptedException()), FOUR_DOT_FOUR_WIRE_VERSION));

        assertTrue(isResumableError(new MongoNotPrimaryException(new BsonDocument(), new ServerAddress()), FOUR_DOT_FOUR_WIRE_VERSION));
        assertTrue(isResumableError(new MongoCursorNotFoundException(1L, new ServerAddress()), FOUR_DOT_FOUR_WIRE_VERSION));
        assertTrue(isResumableError(new MongoSocketException("", new ServerAddress()), FOUR_DOT_FOUR_WIRE_VERSION));
        assertTrue(isResumableError(new MongoSocketReadTimeoutException("", new ServerAddress(), new IOException()), FOUR_DOT_FOUR_WIRE_VERSION));
        assertTrue(isResumableError(new MongoClientException(""), FOUR_DOT_FOUR_WIRE_VERSION));
        assertTrue(isResumableError(new MongoServerUnavailableException(""), FOUR_DOT_FOUR_WIRE_VERSION));

        assertTrue(isResumableError(new MongoCommandException(new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(1000))
                .append("errorLabels", new BsonArray(singletonList(new BsonString(RESUMABLE_CHANGE_STREAM_ERROR_LABEL)))),
                new ServerAddress()), FOUR_DOT_FOUR_WIRE_VERSION));
        assertFalse(isResumableError(new MongoCommandException(new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(RETRYABLE_SERVER_ERROR_CODES.get(0))),
                new ServerAddress()), FOUR_DOT_FOUR_WIRE_VERSION));

        assertTrue(isResumableError(new MongoCommandException(new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(RETRYABLE_SERVER_ERROR_CODES.get(0))),
                new ServerAddress()), FOUR_DOT_TWO_WIRE_VERSION));
    }
}
