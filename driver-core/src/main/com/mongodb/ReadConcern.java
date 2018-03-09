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

import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A read concern allows clients to choose a level of isolation for their reads.
 *
 * @mongodb.server.release 3.2
 * @mongodb.driver.manual reference/readConcern/ Read Concern
 * @since 3.2
 */
public final class ReadConcern {
    private final ReadConcernLevel level;

    /**
     * Construct a new read concern
     *
     * @param level the read concern level
     */
    public ReadConcern(final ReadConcernLevel level) {
        this.level = notNull("level", level);
    }

    /**
     * Use the servers default read concern.
     */
    public static final ReadConcern DEFAULT = new ReadConcern();

    /**
     * The local read concern.
     */
    public static final ReadConcern LOCAL = new ReadConcern(ReadConcernLevel.LOCAL);

    /**
     * The majority read concern.
     */
    public static final ReadConcern MAJORITY = new ReadConcern(ReadConcernLevel.MAJORITY);

    /**
     * The linearizable read concern.
     *
     * <p>
     * This read concern is only compatible with {@link ReadPreference#primary()}.
     * </p>
     *
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public static final ReadConcern LINEARIZABLE = new ReadConcern(ReadConcernLevel.LINEARIZABLE);

    /**
     * Gets the read concern level.
     *
     * @return the read concern level, which may be null (which indicates to use the server's default level)
     * @since 3.6
     */
    @Nullable
    public ReadConcernLevel getLevel() {
        return level;
    }

    /**
     * @return true if this is the server default read concern
     */
    public boolean isServerDefault() {
        return level == null;
    }

    /**
     * Gets this read concern as a document.
     *
     * @return The read concern as a BsonDocument
     */
    public BsonDocument asDocument() {
        BsonDocument readConcern = new BsonDocument();
        if (level != null) {
            readConcern.put("level", new BsonString(level.getValue()));
        }
        return readConcern;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReadConcern that = (ReadConcern) o;

        return level == that.level;
    }

    @Override
    public int hashCode() {
        return level != null ? level.hashCode() : 0;
    }

    private ReadConcern() {
        this.level = null;
    }
}
