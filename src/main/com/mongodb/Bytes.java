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

/**
 * Handles byte functions for <code>ByteEncoder</code> and <code>ByteDecoder</code>.
 */
public class Bytes {
    
    static Logger LOGGER = Logger.getLogger( "com.mongodb" );
    
    static final boolean D = Boolean.getBoolean( "DEBUG.DB" );

    static {
        if ( D )
            LOGGER.setLevel( Level.ALL );
        else
            LOGGER.setLevel( Level.WARNING );
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

    static final byte EOO = 0;    
    static final byte NUMBER = 1;
    static final byte STRING = 2;
    static final byte OBJECT = 3;    
    static final byte ARRAY = 4;
    static final byte BINARY = 5;
    static final byte UNDEFINED = 6;
    static final byte OID = 7;
    static final byte BOOLEAN = 8;
    static final byte DATE = 9;
    static final byte NULL = 10;
    static final byte REGEX = 11;
    static final byte REF = 12;
    static final byte CODE = 13;
    static final byte SYMBOL = 14;
    static final byte CODE_W_SCOPE = 15;
    static final byte NUMBER_INT = 16;
    static final byte TIMESTAMP = 17;
    static final byte NUMBER_LONG = 18;

    static final byte MINKEY = -1;
    static final byte MAXKEY = 127;

    private static final int GLOBAL_FLAG = 256;
    
    
    /* 
       these are binary types
       so the format would look like
       <BINARY><name><BINARY_TYPE><...>
    */

    static final byte B_FUNC = 1;
    static final byte B_BINARY = 2;

    
    static protected Charset _utf8 = Charset.forName( "UTF-8" );
    /** The maximum number of bytes allowed to be sent to the db at a time */
    static protected final int MAX_STRING = 1024 * 512;
    
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

    /** Determines whether the given object was once part of a db collection.
     * This method is not foolproof, the the object has had its _id or _ns fields since
     * it was fetched, this will return that <code>o</code> did not come from the db.
     * @param o the object to check
     * @return if <code>o</code> contains fields that are automatically added by the database on insertion
     */
    public static boolean cameFromDB( DBObject o ){
        if ( o == null )
            return false;

        if ( o.get( "_id" ) == null )
            return false;

        if ( o.get( "_ns" ) == null )
            return false;
        
        return true;
    }

    /** Converts a string of regular expression flags from the database in Java regular
     * expression flags.
     * @param flags flags from database
     * @return the Java flags
     */
    public static int patternFlags( String flags ){
        flags = flags.toLowerCase();
        int fint = 0;

        for( int i=0; i<flags.length(); i++ ) {
            Flag flag = Flag.getByCharacter( flags.charAt( i ) );
            if( flag != null ) {
                fint |= flag.javaFlag;
                if( flag.unsupported != null )
                    _warnUnsupported( flag.unsupported );
            }
            else {
                throw new IllegalArgumentException( "unrecognized flag: "+flags.charAt( i ) );
            }
        }
        return fint;
    }

    /** Converts Java regular expression flags into a string of flags for the database
     * @param flags Java flags
     * @return the flags for the database
     */
    public static String patternFlags( int flags ){
        Flag f[] = Flag.values();
        byte b[] = new byte[ f.length ];
        int count = 0;

        for( Flag flag : f ) {
            if( ( flags & flag.javaFlag ) > 0 ) {
                b[ count++ ] = (byte)flag.flagChar;
                flags -= flag.javaFlag;
            }
        }

        if( flags > 0 )
            throw new IllegalArgumentException( "some flags could not be recognized." );

        Arrays.sort( b );
        return new String( b, 0, count );
    }

    private static enum Flag { 
        CANON_EQ( Pattern.CANON_EQ, 'c', "Pattern.CANON_EQ" ),
        UNIX_LINES(Pattern.UNIX_LINES, 'd', "Pattern.UNIX_LINES" ),
        GLOBAL( GLOBAL_FLAG, 'g', null ),
        CASE_INSENSITIVE( Pattern.CASE_INSENSITIVE, 'i', null ),
        MULTILINE(Pattern.MULTILINE, 'm', null ),
        DOTALL( Pattern.DOTALL, 's', "Pattern.DOTALL" ),
        LITERAL( Pattern.LITERAL, 't', "Pattern.LITERAL" ),
        UNICODE_CASE( Pattern.UNICODE_CASE, 'u', "Pattern.UNICODE_CASE" ),
        COMMENTS( Pattern.COMMENTS, 'x', null );

        private static final Map<Character, Flag> byCharacter = new HashMap<Character, Flag>();

        static {
            for (Flag flag : values()) {
                byCharacter.put(flag.flagChar, flag);
            }
        }

        public static Flag getByCharacter(char ch) {
            return byCharacter.get(ch);
        }
        public final int javaFlag;
        public final char flagChar;
        public final String unsupported;
        Flag( int f, char ch, String u ) {
            javaFlag = f;
            flagChar = ch;
            unsupported = u;
        }
    }

    private static void _warnUnsupported( String flag ) {
        System.out.println( "flag " + flag + " not supported by db." );
    }
    
    static final String NO_REF_HACK = "_____nodbref_____";
    static final ObjectId COLLECTION_REF_ID = new ObjectId( -1 , -1 );
}
