// Bytes.java

package ed.db;

import java.nio.*;
import java.nio.charset.*;

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
        if ( flags.length() == 0 )
            return 0;
        throw new RuntimeException( "can't convert flags yet" );    
    }

    public static String patternFlags( int flags ){
        if ( flags == 0 )
            return "";
        throw new RuntimeException( "can't convert flags yet" );
    }
    
    static final String NO_REF_HACK = "_____nodbref_____";
    static final ObjectId COLLECTION_REF_ID = new ObjectId( -1 , -1 );
}
