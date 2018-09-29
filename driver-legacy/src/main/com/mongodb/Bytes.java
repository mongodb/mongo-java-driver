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

// Bytes.java

package com.mongodb;

import com.mongodb.lang.Nullable;
import org.bson.types.BSONTimestamp;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.ObjectId;

import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class that hold definitions of the wire protocol
 * @deprecated there is no replacement for this class
 */
@Deprecated
@SuppressWarnings("deprecation")
public class Bytes extends org.bson.BSON {

    /**
     * Little-endian
     */
    public static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;


    // --- network protocol options

    /**
     * Tailable means cursor is not closed when the last data is retrieved. Rather, the cursor marks the final object's position. You can
     * resume using the cursor later, from where it was located, if more data were received. Like any "latent cursor", the cursor may become
     * invalid at some point (CursorNotFound) - for example if the final object it references were deleted.
     */
    public static final int QUERYOPTION_TAILABLE = 1 << 1;
    /**
     * When turned on, read queries will be directed to slave servers instead of the primary server.
     */
    public static final int QUERYOPTION_SLAVEOK = 1 << 2;
    /**
     * Internal replication use only - driver should not set
     */
    public static final int QUERYOPTION_OPLOGREPLAY = 1 << 3;
    /**
     * The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to
     * prevent that.
     */
    public static final int QUERYOPTION_NOTIMEOUT = 1 << 4;

    /**
     * Use with TailableCursor. If we are at the end of the data, block for a while rather than returning no data. After a timeout period,
     * we do return as normal.
     */
    public static final int QUERYOPTION_AWAITDATA = 1 << 5;

    /**
     * Stream the data down full blast in multiple "more" packages, on the assumption that the client will fully read all data queried.
     * Faster when you are pulling a lot of data and know you want to pull it all down. Note: the client is not allowed to not read all the
     * data unless it closes the connection.
     */
    public static final int QUERYOPTION_EXHAUST = 1 << 6;

    /**
     * Use with sharding (mongos). Allows partial results from a sharded system if any shards are down/missing from the cluster. If not used
     * an error will be returned from the mongos server.
     */
    public static final int QUERYOPTION_PARTIAL = 1 << 7;

    /**
     * Set when getMore is called but the cursor id is not valid at the server. Returned with zero results.
     */
    public static final int RESULTFLAG_CURSORNOTFOUND = 1;
    /**
     * Set when query failed. Results consist of one document containing an "$err" field describing the failure.
     */
    public static final int RESULTFLAG_ERRSET = 2;
    /**
     * Drivers should ignore this. Only mongos will ever see this set, in which case, it needs to update config from the server.
     */
    public static final int RESULTFLAG_SHARDCONFIGSTALE = 4;
    /**
     * Set when the server supports the AwaitData Query option. If it doesn't, a client should sleep a little between getMore's of a
     * Tailable cursor. Mongod version 1.6 supports AwaitData and thus always sets AwaitCapable.
     */
    public static final int RESULTFLAG_AWAITCAPABLE = 8;


    static class OptionHolder {
        OptionHolder(@Nullable final OptionHolder parent) {
            _parent = parent;
        }

        void set(final int options) {
            _options = options;
            _hasOptions = true;
        }

        int get() {
            if (_hasOptions) {
                return _options;
            }
            if (_parent == null) {
                return 0;
            }
            return _parent.get();
        }

        void add(final int option) {
            set(get() | option);
        }

        void reset() {
            _hasOptions = false;
        }

        final OptionHolder _parent;

        int _options = 0;
        boolean _hasOptions = false;
    }

    /**
     * Gets the type byte for a given object.
     *
     * @param object the object
     * @return the byte value associated with the type, or -1 if no type is matched
     */
    @SuppressWarnings("deprecation")
    public static byte getType(final Object object) {
        if (object == null) {
            return NULL;
        }

        if (object instanceof Integer
            || object instanceof Short
            || object instanceof Byte
            || object instanceof AtomicInteger) {
            return NUMBER_INT;
        }

        if (object instanceof Long || object instanceof AtomicLong) {
            return NUMBER_LONG;
        }

        if (object instanceof Number) {
            return NUMBER;
        }

        if (object instanceof String) {
            return STRING;
        }

        if (object instanceof java.util.List) {
            return ARRAY;
        }

        if (object instanceof byte[]) {
            return BINARY;
        }

        if (object instanceof ObjectId) {
            return OID;
        }

        if (object instanceof Boolean) {
            return BOOLEAN;
        }

        if (object instanceof java.util.Date) {
            return DATE;
        }

        if (object instanceof BSONTimestamp) {
            return TIMESTAMP;
        }

        if (object instanceof java.util.regex.Pattern) {
            return REGEX;
        }

        if (object instanceof DBObject || object instanceof DBRef) {
            return OBJECT;
        }

        if (object instanceof CodeWScope) {
            return CODE_W_SCOPE;
        }

        if (object instanceof Code) {
            return CODE;
        }

        return -1;
    }

}
