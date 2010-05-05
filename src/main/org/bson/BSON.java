// BSON.java

package org.bson;

import java.util.*;
import java.util.regex.*;
import java.nio.charset.*;

public class BSON {

    // ---- basics ----

    public static final byte EOO = 0;    
    public static final byte NUMBER = 1;
    public static final byte STRING = 2;
    public static final byte OBJECT = 3;    
    public static final byte ARRAY = 4;
    public static final byte BINARY = 5;
    public static final byte UNDEFINED = 6;
    public static final byte OID = 7;
    public static final byte BOOLEAN = 8;
    public static final byte DATE = 9;
    public static final byte NULL = 10;
    public static final byte REGEX = 11;
    public static final byte REF = 12;
    public static final byte CODE = 13;
    public static final byte SYMBOL = 14;
    public static final byte CODE_W_SCOPE = 15;
    public static final byte NUMBER_INT = 16;
    public static final byte TIMESTAMP = 17;
    public static final byte NUMBER_LONG = 18;

    public static final byte MINKEY = -1;
    public static final byte MAXKEY = 127;

    // --- binary types
    /* 
       these are binary types
       so the format would look like
       <BINARY><name><BINARY_TYPE><...>
    */
    
    public static final byte B_FUNC = 1;
    public static final byte B_BINARY = 2;

    // ---- regular expression handling ----
    
    /** Converts a string of regular expression flags from the database in Java regular
     * expression flags.
     * @param flags flags from database
     * @return the Java flags
     */
    public static int regexFlags( String flags ){
        flags = flags.toLowerCase();
        int fint = 0;

        for( int i=0; i<flags.length(); i++ ) {
            RegexFlag flag = RegexFlag.getByCharacter( flags.charAt( i ) );
            if( flag != null ) {
                fint |= flag.javaFlag;
                if( flag.unsupported != null )
                    _warnUnsupportedRegex( flag.unsupported );
            }
            else {
                throw new IllegalArgumentException( "unrecognized flag: "+flags.charAt( i ) );
            }
        }
        return fint;
    }

    public static int regexFlag( char c ){
        RegexFlag flag = RegexFlag.getByCharacter( c );
        if ( flag == null )
            throw new IllegalArgumentException( "unrecognized flag: " + c );

        if ( flag.unsupported != null ){
            _warnUnsupportedRegex( flag.unsupported );
            return 0;
        }
        
        return flag.javaFlag;
    }

    /** Converts Java regular expression flags into a string of flags for the database
     * @param flags Java flags
     * @return the flags for the database
     */
    public static String regexFlags( int flags ){
        StringBuilder buf = new StringBuilder();
        
        for( RegexFlag flag : RegexFlag.values() ) {
            if( ( flags & flag.javaFlag ) > 0 ) {
                buf.append( flag.flagChar );
                flags -= flag.javaFlag;
            }
        }

        if( flags > 0 )
            throw new IllegalArgumentException( "some flags could not be recognized." );

        return buf.toString();
    }

    private static enum RegexFlag { 
        CANON_EQ( Pattern.CANON_EQ, 'c', "Pattern.CANON_EQ" ),
        UNIX_LINES(Pattern.UNIX_LINES, 'd', "Pattern.UNIX_LINES" ),
        GLOBAL( GLOBAL_FLAG, 'g', null ),
        CASE_INSENSITIVE( Pattern.CASE_INSENSITIVE, 'i', null ),
        MULTILINE(Pattern.MULTILINE, 'm', null ),
        DOTALL( Pattern.DOTALL, 's', "Pattern.DOTALL" ),
        LITERAL( Pattern.LITERAL, 't', "Pattern.LITERAL" ),
        UNICODE_CASE( Pattern.UNICODE_CASE, 'u', "Pattern.UNICODE_CASE" ),
        COMMENTS( Pattern.COMMENTS, 'x', null );

        private static final Map<Character, RegexFlag> byCharacter = new HashMap<Character, RegexFlag>();

        static {
            for (RegexFlag flag : values()) {
                byCharacter.put(flag.flagChar, flag);
            }
        }

        public static RegexFlag getByCharacter(char ch) {
            return byCharacter.get(ch);
        }
        public final int javaFlag;
        public final char flagChar;
        public final String unsupported;

        RegexFlag( int f, char ch, String u ) {
            javaFlag = f;
            flagChar = ch;
            unsupported = u;
        }
    }

    private static void _warnUnsupportedRegex( String flag ) {
        System.out.println( "flag " + flag + " not supported by db." );
    }

    private static final int GLOBAL_FLAG = 256;

    // --- (en|de)coding hooks -----

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

    private static boolean _anyHooks = false;
    static Map<Class,List<Transformer>> _encodingHooks = 
        Collections.synchronizedMap( new HashMap<Class,List<Transformer>>() );
    static Map<Byte,List<Transformer>> _decodingHooks = 
        Collections.synchronizedMap( new HashMap<Byte,List<Transformer>>() );
    
    static protected Charset _utf8 = Charset.forName( "UTF-8" );

}
