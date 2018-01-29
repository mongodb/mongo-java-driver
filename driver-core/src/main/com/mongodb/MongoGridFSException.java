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

/**
 * An exception indicating that a failure occurred in GridFS.
 *
 * @since 3.1
 */
public class MongoGridFSException extends MongoException {
    private static final long serialVersionUID = -3894346172927543978L;

    /**
     * Constructs a new instance.
     *
     * @param message the message
     */
    public MongoGridFSException(final String message) {
        super(message);
    }

    /**
     * Constructs a new instance.
     *
     * @param message the message
     * @param t       the throwable cause
     */
    public MongoGridFSException(final String message, final Throwable t) {
        super(message, t);
    }
}
