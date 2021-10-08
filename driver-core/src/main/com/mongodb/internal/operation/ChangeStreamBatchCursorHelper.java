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
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;
import com.mongodb.internal.VisibleForTesting;

import java.util.List;

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static com.mongodb.internal.operation.ServerVersionHelper.FOUR_DOT_FOUR_WIRE_VERSION;
import static java.util.Arrays.asList;

final class ChangeStreamBatchCursorHelper {
    @VisibleForTesting(otherwise = PRIVATE)
    static final List<Integer> RETRYABLE_SERVER_ERROR_CODES =
            asList(6, 7, 63, 89, 91, 133, 150, 189, 234, 262, 9001, 10107, 11600, 11602, 13388, 13435, 13436);
    @VisibleForTesting(otherwise = PRIVATE)
    static final String RESUMABLE_CHANGE_STREAM_ERROR_LABEL = "ResumableChangeStreamError";

    static boolean isResumableError(final Throwable t, final int maxWireVersion) {
        if (!(t instanceof MongoException) || (t instanceof MongoChangeStreamException) || (t instanceof MongoInterruptedException)) {
            return false;
        } else if (t instanceof MongoNotPrimaryException || t instanceof MongoCursorNotFoundException
                || t instanceof MongoSocketException | t instanceof MongoClientException) {
            return true;
        } else if (maxWireVersion >= FOUR_DOT_FOUR_WIRE_VERSION) {
            return ((MongoException) t).getErrorLabels().contains(RESUMABLE_CHANGE_STREAM_ERROR_LABEL);
        } else {
            return RETRYABLE_SERVER_ERROR_CODES.contains(((MongoException) t).getCode());
        }
    }

    private ChangeStreamBatchCursorHelper(){
    }
}
