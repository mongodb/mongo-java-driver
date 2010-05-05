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

/**
 * Handles byte functions for <code>ByteEncoder</code> and <code>ByteDecoder</code>.
 */
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
    static final int NUM_ENCODERS;

    static {
        Runtime r = Runtime.getRuntime();
        int numBufs = (int)(r.maxMemory() / MAX_OBJECT_SIZE);
        numBufs = numBufs / 5;
        if ( numBufs > CONNECTIONS_PER_HOST ){
            numBufs = CONNECTIONS_PER_HOST;
        }
        if ( numBufs == 0 )
            throw new IllegalStateException( "the mongo driver doesn't have enough memory to create its buffers" );

        NUM_ENCODERS = numBufs;
    }

    /* 
       these are binary types
       so the format would look like
       <BINARY><name><BINARY_TYPE><...>
    */

    static final byte B_FUNC = 1;
    static final byte B_BINARY = 2;

    public static final int QUERYOPTION_TAILABLE = 1 << 1;
    public static final int QUERYOPTION_SLAVEOK = 1 << 2;
    public static final int QUERYOPTION_OPLOGREPLAY = 1 << 3;
    public static final int QUERYOPTION_NOTIMEOUT = 1 << 4;
    public static final int QUERYOPTION_AWAITDATA = 1 << 5;

    public static final int RESULTFLAG_CURSORNOTFOUND = 1;
    public static final int RESULTFLAG_ERRSET = 2;
    public static final int RESULTFLAG_SHARDCONFIGSTALE = 4;
    public static final int RESULTFLAG_AWAITCAPABLE = 8;

    static protected Charset _utf8 = Charset.forName( "UTF-8" );
    /** The maximum number of bytes allowed to be sent to the db at a time */
    static protected final int MAX_STRING = MAX_OBJECT_SIZE - 1024;
    
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

    public static void addEncodingHook( Class c , Transformer t ){
        _anyHooks = true;
        List<Transformer> l = _encodingHooks.get( c );
        if ( l == null ){
            l = new Vector<Transformer>();
            _encodingHooks.put( c , l );
        }
        l.add( t );
    }
    
    public static void addDecodingHook( byte type , Transformer t ){
        _anyHooks = true;
        List<Transformer> l = _decodingHooks.get( type );
        if ( l == null ){
            l = new Vector<Transformer>();
            _decodingHooks.put( type , l );
        }
        l.add( t );
    }

    public static Object applyEncodingHooks( Object o ){
        if ( ! _anyHooks )
            return o;

        if ( _encodingHooks.size() == 0 || o == null )
            return o;
        List<Transformer> l = _encodingHooks.get( o.getClass() );
        if ( l != null )
            for ( Transformer t : l )
                o = t.transform( o );
        return o;
    }

    public static Object applyDecodingHooks( byte b , Object o ){
        if ( ! _anyHooks )
            return o;

        List<Transformer> l = _decodingHooks.get( b );
        if ( l != null )
            for ( Transformer t : l )
                o = t.transform( o );
        return o;
    }


    public static void clearAllHooks(){
        _anyHooks = false;
        _encodingHooks.clear();
        _decodingHooks.clear();
    }

    public static byte[] encode( DBObject o ){
        ByteEncoder e = ByteEncoder.get();
        e.putObject( o );
        byte b[] = e.getBytes();
        e.done();
        return b;
    }
    
    public static DBObject decode( byte[] b ){
        ByteBuffer bb = ByteBuffer.wrap( b );
        bb.order( Bytes.ORDER );
        ByteDecoder d = new ByteDecoder( bb );
        return d.readObject();
    }

    private static boolean _anyHooks = false;
    static Map<Class,List<Transformer>> _encodingHooks = Collections.synchronizedMap( new HashMap<Class,List<Transformer>>() );
    static Map<Byte,List<Transformer>> _decodingHooks = Collections.synchronizedMap( new HashMap<Byte,List<Transformer>>() );
    
    static final ObjectId COLLECTION_REF_ID = new ObjectId( -1 , -1 , -1 );
}
