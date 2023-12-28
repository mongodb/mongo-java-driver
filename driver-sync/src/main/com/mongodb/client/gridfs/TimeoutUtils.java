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

package com.mongodb.client.gridfs;

import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.client.MongoCollection;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class TimeoutUtils {
    private TimeoutUtils(){
        //NOP
    }

    public static <T> MongoCollection<T> withNullableTimeout(final MongoCollection<T> collection,
                                                                  final String message,
                                                                  @Nullable final Timeout timeout) {
        if (timeout != null) {
            long remainingMs = timeout.remaining(MILLISECONDS);
            if (remainingMs <= 0) {
                // TODO (CSOT) - JAVA-5248 Update to MongoOperationTimeoutException
                throw new MongoExecutionTimeoutException(message);
            }
            return collection.withTimeout(remainingMs, MILLISECONDS);
        }
        return collection;
    }
}
