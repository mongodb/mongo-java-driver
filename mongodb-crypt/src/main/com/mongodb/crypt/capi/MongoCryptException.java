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
 *
 */

package com.mongodb.crypt.capi;


/**
 * Exception thrown for errors originating in the mongodb-crypt module.
 */
public class MongoCryptException extends RuntimeException {
    private static final long serialVersionUID = -5524416583514807953L;
    private final int code;

    /**
     * Construct an instance
     *
     * @param message the message
     * @param code the code
     */
    public MongoCryptException(final String message, final int code) {
        super(message);
        this.code = code;
    }

    /**
     * @param msg the message
     */
    public MongoCryptException(final String msg) {
        super(msg);
        this.code = -1;
    }

    /**
     * @param msg   the message
     * @param cause the cause
     */
    public MongoCryptException(final String msg, final Throwable cause) {
        super(msg, cause);
        this.code = -1;
    }

    /**
     * @return the error code for the exception.
     */
    public int getCode() {
        return code;
    }
}
