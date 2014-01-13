/*
 * Copyright (c) 2008 MongoDB, Inc.
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
 * This exception is thrown if the driver encounters a document that is invalid.  The message will describe the reason.
 *
 * @since 3.0
 */
public class MongoInvalidDocumentException extends MongoClientException {
    private static final long serialVersionUID = 6446682259175856013L;

    /**
     * Constructs a new instance with a message indicating the reason why the document is invalid.
     *
     * @param msg the reason the document is invalid.
     */
    public MongoInvalidDocumentException(final String msg) {
        super(msg);
    }
}
