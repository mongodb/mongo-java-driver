// BSON.java

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

package org.bson;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.bson.util.ClassMap;

public class BSON {

    static final Logger LOGGER = Logger.getLogger( "org.bson.BSON" );

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

    public static final byte B_GENERAL = 0;
    public static final byte B_FUNC = 1;
    public static final byte B_BINARY = 2;
    public static final byte B_UUID = 3;

    // ---- regular expression handling ----

    /** Converts a string of regular expression flags from the database in Java regular
     * expression flags.
     * @param flags flags from database
     * @return the Java flags
     */
    public static int regexFlags( String flags ){
        int fint = 0;
        if ( flags == null || flags.length() == 0 )
            return fint;

        flags = flags.toLowerCase();

        for( int i=0; i<flags.length(); i++ ) {
            RegexFlag flag = RegexFlag.getByCharacter( flags.charAt( i ) );
            if( flag != null ) {
                fint |= flag.javaFlag;
                if( flag.unsupported != null )
                    _warnUnsupportedRegex( flag.unsupported );
            }
            else {
                throw new IllegalArgumentException( "unrecognized flag ["+flags.charAt( i ) + "] " + (int)flags.charAt(i) );
            }
        }
        return fint;
    }

    public static int regexFlag( char c ){
        RegexFlag flag = RegexFlag.getByCharacter( c );
        if ( flag == null )
            throw new IllegalArgumentException( "unrecognized flag [" + c + "]" );

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
        LOGGER.info( "flag " + flag + " not supported by db." );
    }

    private static final int GLOBAL_FLAG = 256;

    // --- (en|de)coding hooks -----

    public static boolean hasDecodeHooks() { return _decodeHooks; }

    public static void addEncodingHook( Class c , Transformer t ){
        _encodeHooks = true;
        List<Transformer> l = _encodingHooks.get( c );
        if ( l == null ){
            l = new CopyOnWriteArrayList<Transformer>();
            _encodingHooks.put( c , l );
        }
        l.add( t );
    }

    public static void addDecodingHook( Class c , Transformer t ){
        _decodeHooks = true;
        List<Transformer> l = _decodingHooks.get( c );
        if ( l == null ){
            l = new CopyOnWriteArrayList<Transformer>();
            _decodingHooks.put( c , l );
        }
        l.add( t );
    }

    public static Object applyEncodingHooks( Object o ){
        if ( ! _anyHooks() )
            return o;

        if ( _encodingHooks.size() == 0 || o == null )
            return o;
        List<Transformer> l = _encodingHooks.get( o.getClass() );
        if ( l != null )
            for ( Transformer t : l )
                o = t.transform( o );
        return o;
    }

    public static Object applyDecodingHooks( Object o ){
        if ( ! _anyHooks() || o == null )
            return o;

        List<Transformer> l = _decodingHooks.get( o.getClass() );
        if ( l != null )
            for ( Transformer t : l )
                o = t.transform( o );
        return o;
    }

   /**
     * Returns the encoding hook(s) associated with the specified class
     *
     */
    public static List<Transformer> getEncodingHooks( Class c ){
        return _encodingHooks.get( c );
    }

    /**
     * Clears *all* encoding hooks.
     */
    public static void clearEncodingHooks(){
        _encodeHooks = false;
        _encodingHooks.clear();
    }

    /**
     * Remove all encoding hooks for a specific class.
     */
    public static void removeEncodingHooks( Class c ){
        _encodingHooks.remove( c );
    }

    /**
     * Remove a specific encoding hook for a specific class.
     */
    public static void removeEncodingHook( Class c , Transformer t ){
        getEncodingHooks( c ).remove( t );
    }

   /**
     * Returns the decoding hook(s) associated with the specific class
     */
    public static List<Transformer> getDecodingHooks( Class c ){
        return _decodingHooks.get( c );
    }

    /**
     * Clears *all* decoding hooks.
     */
    public static void clearDecodingHooks(){
        _decodeHooks = false;
        _decodingHooks.clear();
    }

    /**
     * Remove all decoding hooks for a specific class.
     */
    public static void removeDecodingHooks( Class c ){
        _decodingHooks.remove( c );
    }

    /**
     * Remove a specific encoding hook for a specific class.
     */
    public static void removeDecodingHook( Class c , Transformer t ){
        getDecodingHooks( c ).remove( t );
    }


    public static void clearAllHooks(){
        clearEncodingHooks();
        clearDecodingHooks();
    }

    /**
     * Returns true if any encoding or decoding hooks are loaded.
     */
    private static boolean _anyHooks(){
        return _encodeHooks || _decodeHooks;
    }

    private static boolean _encodeHooks = false;
    private static boolean _decodeHooks = false;
    static ClassMap<List<Transformer>> _encodingHooks =
	new ClassMap<List<Transformer>>();

    static ClassMap<List<Transformer>> _decodingHooks =
        new ClassMap<List<Transformer>>();

    static protected Charset _utf8 = Charset.forName( "UTF-8" );

    // ----- static encode/decode -----

    public static byte[] encode( BSONObject o ){
        BSONEncoder e = _staticEncoder.get();
        try {
            return e.encode( o );
        }
        finally {
            e.done();
        }
    }

    public static BSONObject decode( byte[] b ){
        BSONDecoder d = _staticDecoder.get();
        return d.readObject( b );
    }

    static ThreadLocal<BSONEncoder> _staticEncoder = new ThreadLocal<BSONEncoder>(){
        protected BSONEncoder initialValue(){
            return new BasicBSONEncoder();
        }
    };

    static ThreadLocal<BSONDecoder> _staticDecoder = new ThreadLocal<BSONDecoder>(){
        protected BSONDecoder initialValue(){
            return new BasicBSONDecoder();
        }
    };

    // --- coercing ---

    public static int toInt( Object o ){
        if ( o == null )
            throw new NullPointerException( "can't be null" );

                if ( o instanceof Number )
            return ((Number)o).intValue();

        if ( o instanceof Boolean )
            return ((Boolean)o) ? 1 : 0;

        throw new IllegalArgumentException( "can't convert: " + o.getClass().getName() + " to int" );
    }
}
