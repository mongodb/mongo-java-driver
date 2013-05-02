/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

@SuppressWarnings({ "rawtypes" })
public class BSON {

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

    // --- regex flags

    private static final int FLAG_GLOBAL = 256;

    private static final int[] FLAG_LOOKUP = new int[Character.MAX_VALUE];

    static {
        FLAG_LOOKUP['g'] = FLAG_GLOBAL;
        FLAG_LOOKUP['i'] = Pattern.CASE_INSENSITIVE;
        FLAG_LOOKUP['m'] = Pattern.MULTILINE;
        FLAG_LOOKUP['s'] = Pattern.DOTALL;
        FLAG_LOOKUP['c'] = Pattern.CANON_EQ;
        FLAG_LOOKUP['x'] = Pattern.COMMENTS;
        FLAG_LOOKUP['d'] = Pattern.UNIX_LINES;
        FLAG_LOOKUP['t'] = Pattern.LITERAL;
        FLAG_LOOKUP['u'] = Pattern.UNICODE_CASE;
    }

    private static volatile boolean _encodeHooks = false;
    private static volatile boolean _decodeHooks = false;
    private static final ClassMap<List<Transformer>> _encodingHooks = new ClassMap<List<Transformer>>();
    private static final ClassMap<List<Transformer>> _decodingHooks = new ClassMap<List<Transformer>>();

    public static boolean hasEncodeHooks() {
        return _encodeHooks;
    }

    public static boolean hasDecodeHooks() {
        return _decodeHooks;
    }

    public static void addEncodingHook(final Class c, final Transformer t) {
        _encodeHooks = true;
        List<Transformer> l = _encodingHooks.get(c);
        if (l == null) {
            l = new CopyOnWriteArrayList<Transformer>();
            _encodingHooks.put(c, l);
        }
        l.add(t);
    }

    public static void addDecodingHook(final Class c, final Transformer t) {
        _decodeHooks = true;
        List<Transformer> l = _decodingHooks.get(c);
        if (l == null) {
            l = new CopyOnWriteArrayList<Transformer>();
            _decodingHooks.put(c, l);
        }
        l.add(t);
    }

    public static Object applyEncodingHooks(Object o) {
        if (!hasEncodeHooks() || o == null || _encodingHooks.size() == 0) {
            return o;
        }
        final List<Transformer> l = _encodingHooks.get(o.getClass());
        if (l != null) {
            for (final Transformer t : l) {
                o = t.transform(o);
            }
        }
        return o;
    }

    public static Object applyDecodingHooks(Object o) {
        if (!hasDecodeHooks() || o == null || _decodingHooks.size() == 0) {
            return o;
        }

        final List<Transformer> l = _decodingHooks.get(o.getClass());
        if (l != null) {
            for (final Transformer t : l) {
                o = t.transform(o);
            }
        }
        return o;
    }

    /**
     * Returns the encoding hook(s) associated with the specified class
     */
    public static List<Transformer> getEncodingHooks(final Class c) {
        return _encodingHooks.get(c);
    }

    /**
     * Clears *all* encoding hooks.
     */
    public static void clearEncodingHooks() {
        _encodeHooks = false;
        _encodingHooks.clear();
    }

    /**
     * Remove all encoding hooks for a specific class.
     */
    public static void removeEncodingHooks(final Class c) {
        _encodingHooks.remove(c);
    }

    /**
     * Remove a specific encoding hook for a specific class.
     */
    public static void removeEncodingHook(final Class c, final Transformer t) {
        getEncodingHooks(c).remove(t);
    }

    /**
     * Returns the decoding hook(s) associated with the specific class
     */
    public static List<Transformer> getDecodingHooks(final Class c) {
        return _decodingHooks.get(c);
    }

    /**
     * Clears *all* decoding hooks.
     */
    public static void clearDecodingHooks() {
        _decodeHooks = false;
        _decodingHooks.clear();
    }

    /**
     * Remove all decoding hooks for a specific class.
     */
    public static void removeDecodingHooks(final Class c) {
        _decodingHooks.remove(c);
    }

    /**
     * Remove a specific encoding hook for a specific class.
     */
    public static void removeDecodingHook(final Class c, final Transformer t) {
        getDecodingHooks(c).remove(t);
    }


    public static void clearAllHooks() {
        clearEncodingHooks();
        clearDecodingHooks();
    }

    // ----- static encode/decode -----

    /**
     * Encodes a DBObject as a BSON byte array.
     *
     * @param doc the document to encode
     * @return the document encoded as BSON
     */
    public static byte[] encode(final BSONObject doc) {
        return new BasicBSONEncoder().encode(doc);
    }

    /**
     * Decodes a BSON byte array into a DBObject instance.
     *
     * @param bytes a document encoded as BSON
     * @return the document as a DBObject
     */
    public static BSONObject decode(final byte[] bytes) {
        return new BasicBSONDecoder().readObject(bytes);
    }

    /**
     * Converts a sequence of regular expression modifiers from the database into Java regular
     * expression flags.
     *
     * @param s regular expression modifiers
     * @return the Java flags
     * @throws IllegalArgumentException If sequence contains invalid flags.
     */
    public static int regexFlags(final String s) {
        int flags = 0;

        if (s == null) {
            return flags;
        }

        for (char f : s.toLowerCase().toCharArray()) {
            flags |= regexFlag(f);
        }

        return flags;
    }

    /**
     * Converts a regular expression modifier from the database into Java regular expression flags.
     *
     * @param c regular expression modifier
     * @return the Java flags
     * @throws IllegalArgumentException If sequence contains invalid flags.
     */
    public static int regexFlag(final char c) {

        final int flag = FLAG_LOOKUP[c];

        if (flag == 0) {
            throw new IllegalArgumentException(String.format("Unrecognized flag [%c]", c));
        }

        return flag;
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

        for (int i = 0; i < FLAG_LOOKUP.length; i++) {
            if ((flags & FLAG_LOOKUP[i]) > 0) {
                buf.append((char) i);
                flags -= FLAG_LOOKUP[i];
            }
        }

        if (flags > 0) {
            throw new IllegalArgumentException("Some flags could not be recognized.");
        }

        return buf.toString();
    }
}
