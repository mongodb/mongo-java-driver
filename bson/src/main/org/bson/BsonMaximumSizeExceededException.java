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
 * An exception indicating a failure to serialize a BSON document due to it exceeding the maximum size.
 *
 * @since 3.7
 */
public class BsonMaximumSizeExceededException extends BsonSerializationException {
    private static final long serialVersionUID = 8725368828269129777L;

    /**
     * Construct a new instance.
     *
     * @param message the message
     */
    public BsonMaximumSizeExceededException(final String message) {
        super(message);
    }
}
