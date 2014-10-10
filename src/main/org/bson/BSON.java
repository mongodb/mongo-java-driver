/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.bson;

import org.bson.util.ClassMap;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Contains byte representations of all the BSON types (see the <a href="http://bsonspec.org/spec.html">BSON Specification</a>). Also
 * supports the registration of encoding and decoding hooks to transform BSON types during encoding or decoding.
 *
 * @see org.bson.Transformer
 */
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

    /**
     * Converts a sequence of regular expression modifiers from the database into Java regular expression flags.
     *
     * @param flags regular expression modifiers
     * @return the Java flags
     * @throws IllegalArgumentException If sequence contains invalid flags.
     */
    public static int regexFlags(String flags) {
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

    /**
     * Converts a regular expression modifier from the database into Java regular expression flags.
     *
     * @param c regular expression modifier
     * @return the Java flags
     * @throws IllegalArgumentException If sequence contains invalid flags.
     */
    public static int regexFlag(final char c) {
        RegexFlag flag = RegexFlag.getByCharacter( c );
        if ( flag == null )
            throw new IllegalArgumentException( "unrecognized flag [" + c + "]" );

        if ( flag.unsupported != null ){
            _warnUnsupportedRegex( flag.unsupported );
            return 0;
        }

        return flag.javaFlag;
    }

    /**
     * Converts Java regular expression flags into regular expression modifiers from the database.
     *
     * @param flags the Java flags
     * @return the Java flags
     * @throws IllegalArgumentException if some flags couldn't be recognized.
     */
    public static String regexFlags(int flags) {
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

    /**
     * Gets whether any decoding transformers have been registered for any classes.
     *
     * @return true if any decoding hooks have been registered.
     */
    public static boolean hasDecodeHooks() { return _decodeHooks; }

    /**
     * Registers a {@code Transformer} to use to encode a specific class into BSON.
     *
     * @param c the class to be transformed during encoding
     * @param t the transformer to use during encoding
     */
    public static void addEncodingHook(Class c, Transformer t) {
        _encodeHooks = true;
        List<Transformer> l = _encodingHooks.get( c );
        if ( l == null ){
            l = new CopyOnWriteArrayList<Transformer>();
            _encodingHooks.put( c , l );
        }
        l.add( t );
    }

    /**
     * Registers a {@code Transformer} to use when decoding a specific class from BSON. This class will be one of the basic types supported
     * by BSON.
     *
     * @param c the class to be transformed during decoding
     * @param t the transformer to use during decoding
     */
    public static void addDecodingHook(Class c, Transformer t) {
        _decodeHooks = true;
        List<Transformer> l = _decodingHooks.get( c );
        if ( l == null ){
            l = new CopyOnWriteArrayList<Transformer>();
            _decodingHooks.put( c , l );
        }
        l.add( t );
    }

    /**
     * Transforms the {@code objectToEncode} using all transformers registered for the class of this object.
     *
     * @param o the object being written to BSON.
     * @return the transformed object
     */
    public static Object applyEncodingHooks(Object o) {
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

    /**
     * Transforms the {@code objectToDecode} using all transformers registered for the class of this object.
     *
     * @param o the BSON object to decode
     * @return the transformed object
     */
    public static Object applyDecodingHooks(Object o) {
        if ( ! _anyHooks() || o == null )
            return o;

        List<Transformer> l = _decodingHooks.get( o.getClass() );
        if ( l != null )
            for ( Transformer t : l )
                o = t.transform( o );
        return o;
    }

    /**
     * Returns the encoding hook(s) associated with the specified class.
     *
     * @param clazz the class to fetch the encoding hooks for
     * @return a List of encoding transformers that apply to the given class
     */
    public static List<Transformer> getEncodingHooks(final Class<?> clazz) {
        return _encodingHooks.get(clazz);
    }

    /**
     * Clears <em>all</em> encoding hooks.
     */
    public static void clearEncodingHooks(){
        _encodeHooks = false;
        _encodingHooks.clear();
    }

    /**
     * Remove all encoding hooks for a specific class.
     *
     * @param c the class to remove all the decoding hooks for
     */
    public static void removeEncodingHooks( Class c ){
        _encodingHooks.remove( c );
    }

    /**
     * Remove a specific encoding hook for a specific class. The {@code transformer} passed as the parameter must be {@code equals} to the
     * transformer to remove.
     *
     * @param c the class to remove the encoding hook for
     * @param t the specific encoding hook to remove.
     */
    public static void removeEncodingHook(Class c, Transformer t) {
        getEncodingHooks(c).remove(t);
    }

    /**
     * Returns the decoding hook(s) associated with the specific class
     *
     * @param clazz the class to fetch the decoding hooks for
     * @return a List of all the decoding Transformers that apply to the given class
     */
    public static List<Transformer> getDecodingHooks(final Class clazz) {
        return _decodingHooks.get(clazz);
    }

    /**
     * Clears <em>all</em> decoding hooks.
     */
    public static void clearDecodingHooks(){
        _decodeHooks = false;
        _decodingHooks.clear();
    }

    /**
     * Remove all decoding hooks for a specific class.
     *
     * @param clazz the class to remove all the decoding hooks for
     */
    public static void removeDecodingHooks(Class clazz) {
        _decodingHooks.remove(clazz);
    }

    /**
     * Remove a specific encoding hook for a specific class.  The {@code transformer} passed as the parameter must be {@code equals} to the
     * transformer to remove.
     *
     * @param c the class to remove the decoding hook for
     * @param t the specific decoding hook to remove.
     */
    public static void removeDecodingHook(Class c, Transformer t) {
        getDecodingHooks(c).remove(t);
    }

    /**
     * Remove all decoding and encoding hooks for all classes.
     */
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

    /**
     * @deprecated Use {@link Charset#forName(String)} to create UTF-8 charset.
     */
    @Deprecated
    static protected Charset _utf8 = Charset.forName( "UTF-8" );

    // ----- static encode/decode -----

    /**
     * Encodes a DBObject as a BSON byte array.
     *
     * @param o the document to encode
     * @return the document encoded as BSON
     */
    public static byte[] encode( BSONObject o ){
        BSONEncoder e = _staticEncoder.get();
        try {
            return e.encode( o );
        }
        finally {
            e.done();
        }
    }

    /**
     * Decodes a BSON byte array into a DBObject instance.
     *
     * @param b a document encoded as BSON
     * @return the document as a DBObject
     */
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

    /**
     * Provides an integer representation of Boolean or Number. If argument is {@link Boolean}, then {@code 1} for {@code true} will be
     * returned or @{code 0} otherwise. If argument is {@code Number}, then {@link Number#intValue()} will be called.
     *
     * @param o the number or boolean to convert to an int
     * @return integer value
     * @throws IllegalArgumentException if the argument is {@code null} or not {@link Boolean} or {@link Number}
     */
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
