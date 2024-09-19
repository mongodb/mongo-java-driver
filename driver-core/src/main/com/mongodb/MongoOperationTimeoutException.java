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

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Reason;

import java.util.concurrent.TimeUnit;

/**
 * Exception thrown to indicate that a MongoDB operation has exceeded the specified timeout for
 * the full execution of operation.
 *
 * <p>The {@code MongoOperationTimeoutException} might provide information about the underlying
 * cause of the timeout, if available. For example, if retries are attempted due to transient failures,
 * and a timeout occurs in any of the attempts, the exception from one of the retries may be appended
 * as the cause to this {@code MongoOperationTimeoutException}.
 *
 * <p>The key difference between {@code MongoOperationTimeoutException} and {@code MongoExecutionTimeoutException}
 * lies in the nature of these exceptions. {@code MongoExecutionTimeoutException} indicates a server-side timeout
 * capped by a user-specified number. These server errors are transformed into the new {@code MongoOperationTimeoutException}.
 * On the other hand, {@code MongoOperationExecutionException} denotes a timeout during the execution of the entire operation.
 *
 * @see MongoClientSettings.Builder#timeout(long, TimeUnit)
 * @see MongoClientSettings#getTimeout(TimeUnit)
 * @since 5.2
 */
@Alpha(Reason.CLIENT)
public final class MongoOperationTimeoutException extends MongoTimeoutException {
    private static final long serialVersionUID = 1L;

    /**
     * Construct a new instance.
     *
     * @param message the message
     */
    public MongoOperationTimeoutException(final String message) {
        super(message);
    }

    /**
     * Construct a new instance
     * @param message the message
     * @param cause the cause
     */
    public MongoOperationTimeoutException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
