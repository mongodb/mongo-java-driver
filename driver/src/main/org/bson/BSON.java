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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

@SuppressWarnings("rawtypes")
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

    private static volatile boolean encodeHooks = false;
    private static volatile boolean decodeHooks = false;
    private static final ClassMap<List<Transformer>> encodingHooks = new ClassMap<List<Transformer>>();
    private static final ClassMap<List<Transformer>> decodingHooks = new ClassMap<List<Transformer>>();

    public static boolean hasEncodeHooks() {
        return encodeHooks;
    }

    public static boolean hasDecodeHooks() {
        return decodeHooks;
    }

    public static void addEncodingHook(final Class c, final Transformer t) {
        encodeHooks = true;
        List<Transformer> l = encodingHooks.get(c);
        if (l == null) {
            l = new CopyOnWriteArrayList<Transformer>();
            encodingHooks.put(c, l);
        }
        l.add(t);
    }

    public static void addDecodingHook(final Class c, final Transformer t) {
        decodeHooks = true;
        List<Transformer> l = decodingHooks.get(c);
        if (l == null) {
            l = new CopyOnWriteArrayList<Transformer>();
            decodingHooks.put(c, l);
        }
        l.add(t);
    }

    public static Object applyEncodingHooks(final Object o) {
        Object transfomedObject = o;
        if (!hasEncodeHooks() || o == null || encodingHooks.size() == 0) {
            return transfomedObject;
        }
        List<Transformer> l = encodingHooks.get(o.getClass());
        if (l != null) {
            for (final Transformer t : l) {
                transfomedObject = t.transform(o);
            }
        }
        return transfomedObject;
    }

    public static Object applyDecodingHooks(final Object o) {
        Object transfomedObject = o;
        if (!hasDecodeHooks() || o == null || decodingHooks.size() == 0) {
            return transfomedObject;
        }

        List<Transformer> l = decodingHooks.get(o.getClass());
        if (l != null) {
            for (final Transformer t : l) {
                transfomedObject = t.transform(o);
            }
        }
        return transfomedObject;
    }

    /**
     * Returns the encoding hook(s) associated with the specified class
     */
    public static List<Transformer> getEncodingHooks(final Class c) {
        return encodingHooks.get(c);
    }

    /**
     * Clears *all* encoding hooks.
     */
    public static void clearEncodingHooks() {
        encodeHooks = false;
        encodingHooks.clear();
    }

    /**
     * Remove all encoding hooks for a specific class.
     */
    public static void removeEncodingHooks(final Class c) {
        encodingHooks.remove(c);
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
        return decodingHooks.get(c);
    }

    /**
     * Clears *all* decoding hooks.
     */
    public static void clearDecodingHooks() {
        decodeHooks = false;
        decodingHooks.clear();
    }

    /**
     * Remove all decoding hooks for a specific class.
     */
    public static void removeDecodingHooks(final Class c) {
        decodingHooks.remove(c);
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
     * Converts a sequence of regular expression modifiers from the database into Java regular expression flags.
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

        for (final char f : s.toLowerCase().toCharArray()) {
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

        int flag = FLAG_LOOKUP[c];

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
    public static String regexFlags(final int flags) {
        int processedFlags = flags;
        StringBuilder buf = new StringBuilder();

        for (int i = 0; i < FLAG_LOOKUP.length; i++) {
            if ((processedFlags & FLAG_LOOKUP[i]) > 0) {
                buf.append((char) i);
                processedFlags -= FLAG_LOOKUP[i];
            }
        }

        if (processedFlags > 0) {
            throw new IllegalArgumentException("Some flags could not be recognized.");
        }

        return buf.toString();
    }

    /**
     * Provides an integer representation of Boolean or Number. If argument is {@link Boolean}, then {@code 1} for {@code true} will be
     * returned or @{code 0} otherwise. If argument is {@code Number}, then {@link Number#intValue()} will be called.
     *
     * @param number the number to convert to an int
     * @return integer value
     * @throws IllegalArgumentException if the argument is {@code null} or not {@link Boolean} or {@link Number}
     */
    public static int toInt(final Object number) {
        if (number == null) {
            throw new IllegalArgumentException("Argument shouldn't be null");
        }

        if (number instanceof Number) {
            return ((Number) number).intValue();
        }

        if (number instanceof Boolean) {
            return ((Boolean) number) ? 1 : 0;
        }

        throw new IllegalArgumentException("Can't convert: " + number.getClass().getName() + " to int");
    }
}
