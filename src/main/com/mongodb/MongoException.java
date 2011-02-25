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

import org.bson.*;

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
        super( msg , _massage( t ) );
        _code = -4;
    }

    /**
     *
     * @param code the error code
     * @param msg the message
     * @param t the throwable cause
     */
    public MongoException( int code , String msg , Throwable t ){
        super( msg , _massage( t ) );
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


    static Throwable _massage( Throwable t ){
        if ( t instanceof Network )
            return ((Network)t)._ioe;
        return t;
    }

    /**
     * Subclass of MongoException representing a network-related exception
     */
    public static class Network extends MongoException {

        private static final long serialVersionUID = -4415279469780082174L;

        Network( String msg , java.io.IOException ioe ){
            super( -2 , msg , ioe );
            _ioe = ioe;
        }

        Network( java.io.IOException ioe ){
            super( ioe.toString() , ioe );
            _ioe = ioe;
        }
        
        final java.io.IOException _ioe;
    }

    /**
     * Subclass of MongoException representing a duplicate key exception
     */
    public static class DuplicateKey extends MongoException {

        private static final long serialVersionUID = -4415279469780082174L;

        DuplicateKey( int code , String msg ){
            super( code , msg );
        }
    }

    /**
     * Subclass of MongoException representing a cursor-not-found exception
     */
    public static class CursorNotFound extends MongoException {
        
        private static final long serialVersionUID = -4415279469780082174L;

        CursorNotFound(){
            super( -5 , "cursor not found on server" );
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
