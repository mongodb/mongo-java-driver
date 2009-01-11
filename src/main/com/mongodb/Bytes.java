// Bytes.java

package com.mongodb;

import java.nio.*;
import java.nio.charset.*;
import java.util.regex.Pattern;
import java.util.Arrays;

/**
 * <type><name>0<data>
 *   <NUMBER><name>0<double>
 *   <STRING><name>0<len><string>0
 
 */
public class Bytes {

    static final boolean D = Boolean.getBoolean( "DEBUG.DB" );

    public static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;

    static final int BUF_SIZE = 1024 * 1024 * 5;
    
    static final int CONNECTIONS_PER_HOST = 10;
    static final int BUFS_PER_50M = ( 1024 * 1024 * 50 ) / BUF_SIZE;

    static final byte EOO = 0;    
    static final byte MAXKEY = -1;
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
    
    
    /* 
       these are binary types
       so the format would look like
       <BINARY><name><BINARY_TYPE><...>
    */

    static final byte B_FUNC = 1;
    static final byte B_BINARY = 2;

    
    static protected Charset _utf8 = Charset.forName( "UTF-8" );
    static protected final int MAX_STRING = 1024 * 512;
    
    public static byte getType( Object o ){
        if ( o == null )
            return NULL;

        if ( o instanceof DBRef )
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

    public static boolean cameFromDB( DBObject o ){
        if ( o == null )
            return false;

        if ( o.get( "_id" ) == null )
            return false;

        if ( o.get( "_ns" ) == null )
            return false;
        
        return true;
    }

    public static int patternFlags( String flags ){
        flags = flags.toLowerCase();
        Flag f[] = Flag.values();
        int fint = 0;
        int count = 0;

        for( Flag flag : f ) {
            if( flags.indexOf( flag.flagChar ) >= 0 ) {
                fint |= flag.javaFlag;
                count++;
                if( flag.unsupported != null )
                    _warnUnsupported( flag.unsupported );
            }
        }

        if( count < flags.length() )
            throw new RuntimeException( "some flags could not be converted: "+flags );

        return fint;
    }

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
            throw new RuntimeException( "some flags could not be recognized." );

        Arrays.sort( b );
        return new String( b, 0, count );
    }

    private static enum Flag { 
        CANON_EQ( Pattern.CANON_EQ, 'c', "PATTERN.CANON_EQ" ),
        DOTALL( Pattern.DOTALL, 'd', "Pattern.DOTALL" ),
        CASE_INSENSITIVE( Pattern.CASE_INSENSITIVE, 'i', null ),
        UNIX_LINES( Pattern.UNIX_LINES, 'l', "Pattern.UNIX_LINES" ),
        MULTILINE(Pattern.MULTILINE, 'm', null ),
        LITERAL( Pattern.LITERAL, 't', "Pattern.LITERAL" ),
        UNICODE_CASE( Pattern.UNICODE_CASE, 'u', "Pattern.UNICODE_CASE" ),
        COMMENTS( Pattern.COMMENTS, 'x', null );

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
