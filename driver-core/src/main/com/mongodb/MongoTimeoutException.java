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
import com.mongodb.lang.Nullable;

/**
 * An exception indicating that the driver has timed out waiting for either a server or a connection to become available.
 */
public class MongoTimeoutException extends MongoClientException {

    private static final long serialVersionUID = -3016560214331826577L;

    /**
     * Construct a new instance.
     *
     * @param message the message
     */
    public MongoTimeoutException(final String message) {
        super(message);
    }

    /**
     * Construct a new instance
     * @param message the message
     * @param cause the cause
     * @since 5.2
     */
    @Alpha(Reason.CLIENT)
    public MongoTimeoutException(final String message, @Nullable final Throwable cause) {
        super(message, cause);
    }
}
