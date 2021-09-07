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

import com.mongodb.lang.Nullable;

/**
 * A base class for exceptions indicating a failure condition with the MongoClient.
 *
 * @since 2.12
 */
public class MongoClientException extends MongoException {

    private static final long serialVersionUID = -5127414714432646066L;

    /**
     * Constructs a new instance.
     *
     * @param message the message
     */
    public MongoClientException(final String message) {
        super(message);
    }

    /**
     * Constructs a new instance.
     *
     * @param message the message
     * @param cause the cause
     */
    public MongoClientException(final String message, @Nullable final Throwable cause) {
        super(message, cause);
    }
}
