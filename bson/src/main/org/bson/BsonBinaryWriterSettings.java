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
 * The customisable settings for writing BSON.
 *
 * @since 3.0
 */
public class BsonBinaryWriterSettings {
    private final int maxDocumentSize;

    /**
     * Creates a new instance of the settings with the given maximum document size.
     *
     * @param maxDocumentSize the maximum document size.
     */
    public BsonBinaryWriterSettings(final int maxDocumentSize) {
        this.maxDocumentSize = maxDocumentSize;
    }

    /**
     * Creates a new instance of the settings with {@link java.lang.Integer#MAX_VALUE} as the maximum document size.
     */
    public BsonBinaryWriterSettings() {
        this(Integer.MAX_VALUE);
    }

    /**
     * Gets the maximum size for BSON documents.
     *
     * @return the maximum size of BSON documents. ???
     */
    public int getMaxDocumentSize() {
        return maxDocumentSize;
    }
}
