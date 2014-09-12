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

import java.util.EnumSet;

/**
 * An enumeration of all query options supported by the OP_QUERY wire protocol message.
 *
 * @since 3.0
 */
public enum CursorFlag {
    /**
     * Tailable means cursor is not closed when the last data is retrieved. Rather, the cursor marks the final object's position. You can
     * resume using the cursor later, from where it was located, if more data were received. Like any "latent cursor", the cursor may become
     * invalid at some point (CursorNotFound) – for example if the final object it references were deleted.
     */
    Tailable(1 << 1),

    /**
     * When turned on, .
     */
    SLAVE_OK(1 << 2),

    /**
     * Internal replication use only - driver should not set
     */
    OPLOG_REPLAY(1 << 3),

    /**
     * The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to
     * prevent that.
     */
    NO_CURSOR_TIMEOUT(1 << 4),

    /**
     * Use with TailableCursor. If we are at the end of the data, block for a while rather than returning no data. After a timeout period,
     * we do return as normal.
     */
    AWAIT_DATA(1 << 5),

    /**
     * Stream the data down full blast in multiple "more" packages, on the assumption that the client will fully read all data queried.
     * Faster when you are pulling a lot of data and know you want to pull it all down. Note: the client is not allowed to not read all the
     * data unless it closes the connection.
     */
    EXHAUST(1 << 6),

    /**
     * Use with sharding (mongos). Allows partial results from a sharded system if any shards are down/missing from the cluster. If not used
     * an error will be returned from the mongos server.
     */
    PARTIAL(1 << 7);

    private final int bit;

    /**
     * @param queryOptions the query options as a bit set
     * @return The set of QueryFlag specified by the bits set in queryOptions
     */
    public static EnumSet<CursorFlag> toSet(final int queryOptions) {
        EnumSet<CursorFlag> retVal = EnumSet.noneOf(CursorFlag.class);
        for (final CursorFlag flag : CursorFlag.values()) {
            if ((queryOptions & flag.getBit()) != 0) {
                retVal.add(flag);
            }
        }
        return retVal;
    }

    /**
     * Returns an integer with the bits set for the options included in the given set.
     *
     * @param cursorFlagSet the set of query options
     * @return an integer with all bits set for the options included in the given set
     */
    public static int fromSet(final EnumSet<CursorFlag> cursorFlagSet) {
        int retVal = 0;
        for (final CursorFlag flag : cursorFlagSet) {
            retVal |= flag.getBit();
        }
        return retVal;
    }

    /**
     * Returns an integer with a single bit set for this query options.
     *
     * @return an integer with the bit set for this query option.
     */
    public int getBit() {
        return bit;
    }

    private CursorFlag(final int bit) {
        this.bit = bit;
    }

}
