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

package com.mongodb.embedded.client;

import com.mongodb.MongoException;

import static java.lang.String.format;

/**
 * Exceptions indicating a failure condition with the embedded MongoClient.
 *
 * @since 3.8
 */
public final class MongoClientEmbeddedException extends MongoException {
    private static final long serialVersionUID = 8314840681404996248L;

    /**
     * Constructs a new instance
     *
     * @param errorCode the error code
     * @param subErrorCode the sub category error code
     * @param reason the reason for the exception
     */
    public MongoClientEmbeddedException(final int errorCode, final int subErrorCode, final String reason) {
        super(errorCode, format("%s (%s:%s)", reason, errorCode, subErrorCode));
    }

    /**
     * Constructs a new instance.
     *
     * @param message the message
     */
    public MongoClientEmbeddedException(final String message) {
        super(message);
    }

    /**
     * Constructs a new instance.
     *
     * @param message the message
     * @param cause the cause
     */
    public MongoClientEmbeddedException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
