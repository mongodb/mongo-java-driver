/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
 * This exception is thrown if the application attempts to get information about the result of an unacknowledged write.
 *
 * @see WriteConcern#UNACKNOWLEDGED
 * @see WriteResult
 * @see BulkWriteResult
 * @since 2.12
 */
public class UnacknowledgedWriteException extends MongoClientException {

    private static final long serialVersionUID = 6974332938681213965L;

    /**
     * Constructs a new instance with the given message.
     *
     * @param msg the message
     */
    UnacknowledgedWriteException(final String msg) {
        super(msg);
    }
}
