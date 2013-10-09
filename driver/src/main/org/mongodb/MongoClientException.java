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

package org.mongodb;

/**
 * A base class for exceptions indicating a failure condition within the driver.
 */
public abstract class MongoClientException extends MongoException {
    private static final long serialVersionUID = -6156258093031308896L;

    /**
     * Constructs a new instance with the given message.
     *
     * @param msg the message
     */
    public MongoClientException(final String msg) {
        super(msg);
    }

    /**
     * Constructs a new instance with the given message and chained exception.
     *
     * @param msg the message
     * @param t   the chained exception
     */
    public MongoClientException(final String msg, final Throwable t) {
        super(msg, t);
    }
}
