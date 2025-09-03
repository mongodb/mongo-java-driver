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

/**
 * This exception is thrown when there is a timeout writing a response from the socket.
 *
 * @since 5.2
 */
@Alpha(Reason.CLIENT)
public class MongoSocketWriteTimeoutException extends MongoSocketException {

    private static final long serialVersionUID = 1L;

    /**
     * Construct a new instance
     *
     * @param message the message
     * @param address the address
     * @param cause the cause
     */
    public MongoSocketWriteTimeoutException(final String message, final ServerAddress address, final Throwable cause) {
        super(message, address, cause);
    }

}
