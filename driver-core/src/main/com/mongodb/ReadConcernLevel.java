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

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * A read concern level allows clients to choose a level of isolation for their reads.
 *
 * @mongodb.server.release 3.2
 * @mongodb.driver.manual reference/read-concern/#read-concern-levels Read Concern Levels
 * @since 3.2
 */
public enum ReadConcernLevel {

    /**
     * The local read concern level.
     */
    LOCAL("local"),

    /**
     * The majority read concern level.
     */
    MAJORITY("majority"),

    /**
     * The linearizable read concern level.
     *
     * <p>
     * This read concern level is only compatible with {@link ReadPreference#primary()}.
     * </p>
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
