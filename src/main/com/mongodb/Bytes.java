// Bytes.java

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

import java.nio.*;
import java.nio.charset.*;
import java.util.regex.Pattern;
import java.util.*;
import java.util.logging.*;

import org.bson.*;
import org.bson.types.*;

public class Bytes extends BSON {
    
    static Logger LOGGER = Logger.getLogger( "com.mongodb" );
    
    static final boolean D = Boolean.getBoolean( "DEBUG.MONGO" );

    static {
        if ( LOGGER.getLevel() == null ){
            if ( D )
                LOGGER.setLevel( Level.ALL );
            else
                LOGGER.setLevel( Level.WARNING );
        }
    }

    /** Little-endian */
    public static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;
    
    static final int MAX_OBJECT_SIZE = 1024 * 1024 * 4;
    
    static final int CONNECTIONS_PER_HOST = Integer.parseInt( System.getProperty( "MONGO.POOLSIZE" , "10" ) );


    // --- network protocol options

    public static final int QUERYOPTION_TAILABLE = 1 << 1;
    public static final int QUERYOPTION_SLAVEOK = 1 << 2;
    public static final int QUERYOPTION_OPLOGREPLAY = 1 << 3;
    public static final int QUERYOPTION_NOTIMEOUT = 1 << 4;
    public static final int QUERYOPTION_AWAITDATA = 1 << 5;

    public static final int RESULTFLAG_CURSORNOTFOUND = 1;
    public static final int RESULTFLAG_ERRSET = 2;
    public static final int RESULTFLAG_SHARDCONFIGSTALE = 4;
    public static final int RESULTFLAG_AWAITCAPABLE = 8;

    static class OptionHolder {
        OptionHolder( OptionHolder parent ){
            _parent = parent;
        }

        void set( int options ){
            _options = options;
            _hasOptions = true;
        }
        
        int get(){
            if ( _hasOptions )
                return _options;
            if ( _parent == null )
                return 0;
            return _parent.get();
        }
        
        void add( int option ){
            set( get() | option );
        }

        void reset(){
            _hasOptions = false;
        }

        final OptionHolder _parent;
        
        int _options = 0;
        boolean _hasOptions = false;
    }

    /** Gets the type byte for a given object.
     * @param o the object
     * @return the byte value associated with the type, or 0 if <code>o</code> was <code>null</code>
     */
    public static byte getType( Object o ){
        if ( o == null )
            return NULL;

        if ( o instanceof DBPointer )
            return REF;

        if ( o instanceof Number )
            return NUMBER;
        
        if ( o instanceof String )
            return STRING;
        
        if ( o instanceof java.util.List )
            return ARRAY;

        if ( o instanceof byte[] )
            return BINARY;

        if ( o instanceof ObjectId )
            return OID;
        
        if ( o instanceof Boolean )
            return BOOLEAN;
        
        if ( o instanceof java.util.Date )
            return DATE;

        if ( o instanceof java.util.regex.Pattern )
            return REGEX;
        
        if ( o instanceof DBObject )
            return OBJECT;

        return 0;
    }

    static final ObjectId COLLECTION_REF_ID = new ObjectId( -1 , -1 , -1 );
}
