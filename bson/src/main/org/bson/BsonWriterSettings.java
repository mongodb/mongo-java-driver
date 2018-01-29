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
 * All the customisable settings for writing BSON.
 *
 * @since 3.0
 */
public class BsonWriterSettings {
    private final int maxSerializationDepth;

    /**
     * Creates a new instance of the settings with the given maximum serialization depth.
     *
     * @param maxSerializationDepth the maximum number of nested levels to serialise
     */
    public BsonWriterSettings(final int maxSerializationDepth) {
        this.maxSerializationDepth = maxSerializationDepth;
    }

    /**
     * Creates a new instance of the settings with the default maximum serialization depth of 1024.
     */
    public BsonWriterSettings() {
        this(1024);
    }

    /**
     * Gets the maximum nuber of levels of depth defined by this settings object.
     *
     * @return the maximum number of levels that can be serialized.
     */
    public int getMaxSerializationDepth() {
        return maxSerializationDepth;
    }
}
