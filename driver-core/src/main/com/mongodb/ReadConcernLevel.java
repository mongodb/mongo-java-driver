/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * The readConcern option allows clients to choose a level of isolation for their reads.
 *
 * The WiredTiger storage engine, introduces the readConcern option for replica sets and replica set shards.  Allowing clients to choose a
 * level of isolation for their reads.
 *
 * @mongodb.server.release 3.2
 * @mongodb.driver.manual reference/readConcern/ Read Concern
 * @since 3.2
 */
public enum ReadConcernLevel {

    /**
     * Return the node's most recent copy of data. Provides no guarantee that the data has been written to a majority of the nodes.
     */
    LOCAL("local"),

    /**
     * Return the node's most recent copy of the data confirmed as having been written to a majority of the nodes.
     */
    MAJORITY("majority"),

    /**
     * Return the most recent copy of the data that will not be rolled back.
     *
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    LINEARIZABLE("linearizable");

    private final String value;
    ReadConcernLevel(final String readConcernLevel) {
        this.value = readConcernLevel;
    }

    /**
     * @return the String representation of the read concern level that the MongoDB server understands or null for the default
     */
    public String getValue() {
        return value;
    }

    /**
     * Returns the ReadConcern from the string read concern level.
     *
     * @param readConcernLevel the read concern level string.
     * @return the read concern
     */
    public static ReadConcernLevel fromString(final String readConcernLevel) {
        notNull("readConcernLevel", readConcernLevel);
        for (ReadConcernLevel level : ReadConcernLevel.values()) {
            if (readConcernLevel.equalsIgnoreCase(level.value)) {
                return level;
            }
        }
        throw new IllegalArgumentException(format("'%s' is not a valid readConcernLevel", readConcernLevel));
    }
}
