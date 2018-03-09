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
 * Top level Exception for all Exceptions, server-side or client-side, that come from the driver.
 */
public class MongoException extends RuntimeException {
    private static final long serialVersionUID = -4415279469780082174L;

    private final int code;

    /**
     * Static helper to create or cast a MongoException from a throwable
     *
     * @param t a throwable, which may be null
     * @return a MongoException
     */
    @Nullable
    public static MongoException fromThrowable(@Nullable final Throwable t) {
        if (t == null) {
            return null;
        } else {
            return fromThrowableNonNull(t);
        }
    }

    /**
     * Static helper to create or cast a MongoException from a throwable
     *
     * @param t a throwable, which may not be null
     * @return a MongoException
     * @since 3.7
     */
    public static MongoException fromThrowableNonNull(final Throwable t) {
      if (t instanceof MongoException) {
            return (MongoException) t;
        } else {
            return new MongoException(t.getMessage(), t);
        }
    }

    /**
     * @param msg the message
     */
    public MongoException(final String msg) {
        super(msg);
        code = -3;
    }

    /**
     * @param code the error code
     * @param msg  the message
     */
    public MongoException(final int code, final String msg) {
        super(msg);
        this.code = code;
    }

    /**
     * @param msg the message
     * @param t   the throwable cause
     */
    public MongoException(final String msg, final Throwable t) {
        super(msg, t);
        code = -4;
    }

    /**
     * @param code the error code
     * @param msg  the message
     * @param t    the throwable cause
     */
    public MongoException(final int code, final String msg, final Throwable t) {
        super(msg, t);
        this.code = code;
    }

    /**
     * Gets the exception code
     *
     * @return the error code.
     */
    public int getCode() {
        return code;
    }
}
