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

package org.bson;

/**
 * A general runtime exception raised in BSON processing.
 * @serial exclude
 */
public class BSONException extends RuntimeException {

    private static final long serialVersionUID = -4415279469780082174L;

    private Integer errorCode = null;

    /**
     * @param msg The error message.
     */
    public BSONException(final String msg) {
        super(msg);
    }

    /**
     * @param errorCode The error code.
     * @param msg       The error message.
     */
    public BSONException(final int errorCode, final String msg) {
        super(msg);
        this.errorCode = errorCode;
    }

    /**
     * @param msg The error message.
     * @param t   The throwable cause.
     */
    public BSONException(final String msg, final Throwable t) {
        super(msg, t);
    }

    /**
     * @param errorCode The error code.
     * @param msg       The error message.
     * @param t         The throwable cause.
     */
    public BSONException(final int errorCode, final String msg, final Throwable t) {
        super(msg, t);
        this.errorCode = errorCode;
    }

    /**
     * Returns the error code.
     *
     * @return The error code.
     */
    public Integer getErrorCode() {
        return errorCode;
    }

    /**
     * Returns if the error code is set (i.e., not null).
     *
     * @return true if the error code is not null.
     */
    public boolean hasErrorCode() {
        return (errorCode != null);
    }
}

