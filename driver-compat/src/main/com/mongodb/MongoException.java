/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.bson.BSONObject;
import org.mongodb.command.MongoDuplicateKeyException;
import org.mongodb.operation.MongoCursorNotFoundException;
import org.mongodb.operation.MongoServerException;

/**
 * Top level Exception for all Exceptions, server-side or client-side, that come from the driver.
 */
public class MongoException extends RuntimeException {
    private static final long serialVersionUID = -4415279469780082174L;

    private final int code;

    public MongoException(final org.mongodb.MongoException e) {
        super("Chained exception", e);
        if (e instanceof MongoServerException) {
            code = ((MongoServerException) e).getErrorCode();
        }
        else {
            code = -1;
        }
    }

    /**
     * @param msg the message
     */
    public MongoException(final String msg) {
        super(msg);
        code = -3;
    }

    /**
     * @param code the error code
     * @param msg  the message
     */
    public MongoException(final int code, final String msg) {
        super(msg);
        this.code = code;
    }

    /**
     * @param msg the message
     * @param t   the throwable cause
     */
    public MongoException(final String msg, final Throwable t) {
        super(msg, _massage(t));
        code = -4;
    }

    /**
     * @param code the error code
     * @param msg  the message
     * @param t    the throwable cause
     */
    public MongoException(final int code, final String msg, final Throwable t) {
        super(msg, _massage(t));
        this.code = code;
    }

    /**
     * Creates a MongoException from a BSON object representing an error
     *
     * @param o a BSONObject representing the MongoException to be thrown.
     */
    public MongoException(final BSONObject o) {
        this(ServerError.getCode(o), ServerError.getMsg(o, "UNKNOWN"));
    }

    static MongoException parse(final BSONObject o) {
        final String s = ServerError.getMsg(o, null);
        if (s == null) {
            return null;
        }
        return new MongoException(ServerError.getCode(o), s);
    }


    static Throwable _massage(final Throwable t) {
        if (t instanceof Network) {
            return ((Network) t)._ioe;
        }
        return t;
    }

    /**
     * Subclass of MongoException representing a network-related exception
     */
    public static class Network extends MongoException {
        private static final long serialVersionUID = -4415279469780082174L;

        private final java.io.IOException _ioe;

        /**
         * @param msg the message
         * @param ioe the cause
         */
        public Network(final String msg, final java.io.IOException ioe) {
            super(-2, msg, ioe);
            _ioe = ioe;
        }

        /**
         * @param ioe the cause
         */
        public Network(final java.io.IOException ioe) {
            super(ioe.toString(), ioe);
            _ioe = ioe;
        }

    }

    /**
     * Subclass of MongoException representing a duplicate key exception
     */
    public static class DuplicateKey extends MongoException {

        private static final long serialVersionUID = -4415279469780082174L;

        /**
         * Chaining the exception - this constructor will take all relevant values from the original MongoDuplicateKeyException and put
         * them into this DuplicateKey, but all reference to the MongoDuplicateKeyException will be removed from the stack trace.  This
         * is so that we don't leak the exceptions from the org.mongodb layer.
         *
         * @param e the exception from the new Java layer
         */
        public DuplicateKey(final MongoDuplicateKeyException e) {
            super(e.getCommandResult().getErrorCode(), e.getMessage(), e.getCause());
        }

        /**
         * @param code the error code
         * @param msg  the message
         */
        public DuplicateKey(final int code, final String msg) {
            super(code, msg);
        }
    }

    /**
     * Subclass of MongoException representing a cursor-not-found exception
     */
    public static class CursorNotFound extends MongoException {

        private static final long serialVersionUID = -4415279469780082174L;

        private final long cursorId;
        private final ServerAddress serverAddress;

        /**
         * @param cursorId      cursor
         * @param serverAddress server address
         */
        public CursorNotFound(final long cursorId, final ServerAddress serverAddress) {
            super(-5, "cursor " + cursorId + " not found on server " + serverAddress);
            this.cursorId = cursorId;
            this.serverAddress = serverAddress;
        }

        CursorNotFound(final MongoCursorNotFoundException e) {
            this(e.getCursor().getId(), new ServerAddress(e.getCursor().getAddress()));
        }

        /**
         * Get the cursor id that wasn't found.
         *
         * @return the ID of the cursor
         */
        public long getCursorId() {
            return cursorId;
        }

        /**
         * The server address where the cursor is.
         *
         * @return the ServerAddress representing the server the cursor was on.
         */
        public ServerAddress getServerAddress() {
            return serverAddress;
        }
    }

    /**
     * Gets the exception code
     *
     * @return the error code.
     */
    public int getCode() {
        return code;
    }

}
