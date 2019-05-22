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

package com.mongodb.operation;

import com.mongodb.MongoChangeStreamException;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;

import java.util.List;

import static java.util.Arrays.asList;

final class ChangeStreamBatchCursorHelper {
    private static final List<Integer> UNRETRYABLE_SERVER_ERROR_CODES = asList(136, 237, 280, 11601);

    static boolean isRetryableError(final Throwable t) {
        if (!(t instanceof MongoException) || t instanceof MongoChangeStreamException) {
            return false;
        } else if (t instanceof MongoNotPrimaryException || t instanceof MongoCursorNotFoundException
                || t instanceof MongoSocketException) {
            return true;
        } else {
            return !UNRETRYABLE_SERVER_ERROR_CODES.contains(((MongoException) t).getCode());
        }
    }

    private ChangeStreamBatchCursorHelper(){
    }
}
