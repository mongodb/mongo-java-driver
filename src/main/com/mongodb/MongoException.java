// MongoException.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb;

import org.bson.BSONObject;

import java.io.IOException;

/**
 * A general exception raised in Mongo
 * @author antoine
 */
public class MongoException extends RuntimeException {

    private static final long serialVersionUID = -4415279469780082174L;

    /**
     * @param msg the message
     */
    public MongoException( String msg ){
        super( msg );
        _code = -3;
    }

    /**
     *
     * @param code the error code
     * @param msg the message
     */
    public MongoException( int code , String msg ){
        super( msg );
        _code = code;
    }

    /**
     *
     * @param msg the message
     * @param t the throwable cause
     */
    public MongoException( String msg , Throwable t ){
        super( msg , t );
        _code = -4;
    }

    /**
     *
     * @param code the error code
     * @param msg the message
     * @param t the throwable cause
     */
    public MongoException( int code , String msg , Throwable t ){
        super( msg , t );
        _code = code;
    }

    /**
     * Creates a MongoException from a BSON object representing an error
     * @param o
     */
    public MongoException( BSONObject o ){
        this( ServerError.getCode( o ) , ServerError.getMsg( o , "UNKNOWN" ) );
    }

    static MongoException parse( BSONObject o ){
        String s = ServerError.getMsg( o , null );
        if ( s == null )
            return null;
        return new MongoException( ServerError.getCode( o ) , s );
    }

    /**
     * Subclass of MongoException representing a network-related exception.
     *
     * @deprecated This class will be dropped in 3.x versions.
     *             Please catch {@link MongoSocketException} instead.
     */
    @Deprecated
    public static class Network extends MongoSocketException {

        private static final long serialVersionUID = 8364298902504372967L;

        /**
         * @param msg the message
         * @param ioe the cause
         */
        public Network(String msg, IOException ioe) {
            super(msg, ioe);
        }

        /**
         * @param ioe the cause
         */
        public Network(IOException ioe) {
            super(ioe);
        }
    }

    /**
     * Subclass of WriteConcernException representing a duplicate key error.
     *
     * @deprecated This class will be dropped in 3.x versions.
     *             Please catch {@link MongoDuplicateKeyException} instead.
     */
    @Deprecated
    public static class DuplicateKey extends MongoDuplicateKeyException {

        private static final long serialVersionUID = 6557680785576001838L;

        public DuplicateKey(final CommandResult commandResult) {
            super(commandResult);
        }
    }

    /**
     * Subclass of MongoException representing a cursor-not-found exception
     *
     * @deprecated This class will be dropped in 3.x versions.
     *             Please catch {@link MongoCursorNotFoundException} instead.
     */
    @Deprecated
    public static class CursorNotFound extends MongoCursorNotFoundException {

        private static final long serialVersionUID = -3759595395830412426L;

        /**
         * @param cursorId      cursor
         * @param serverAddress server address
         */
        public CursorNotFound(long cursorId, ServerAddress serverAddress) {
            super(cursorId, serverAddress);
        }
    }

    /**
     * Gets the exception code
     * @return
     */
    public int getCode(){
        return _code;
    }

    final int _code;
}
