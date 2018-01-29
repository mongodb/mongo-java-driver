/*
 * Copyright 2008-present MongoDB, Inc.
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

/**
 * Contains byte representations of all the BSON types (see the <a href="http://bsonspec.org/spec.html">BSON Specification</a>). Also
 * supports the registration of encoding and decoding hooks to transform BSON types during encoding or decoding.
 *
 * @see org.bson.Transformer
 */
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

    /**
     * Gets whether any encoding transformers have been registered for any classes.
     *
     * @return true if any encoding hooks have been registered.
     */
    public static boolean hasEncodeHooks() {
        return encodeHooks;
    }

    /**
     * Gets whether any decoding transformers have been registered for any classes.
     *
     * @return true if any decoding hooks have been registered.
     */
    public static boolean hasDecodeHooks() {
        return decodeHooks;
    }

    /**
     * Registers a {@code Transformer} to use to encode a specific class into BSON.
     *
     * @param clazz       the class to be transformed during encoding
     * @param transformer the transformer to use during encoding
     */
    public static void addEncodingHook(final Class<?> clazz, final Transformer transformer) {
        encodeHooks = true;
        List<Transformer> transformersForClass = encodingHooks.get(clazz);
        if (transformersForClass == null) {
            transformersForClass = new CopyOnWriteArrayList<Transformer>();
            encodingHooks.put(clazz, transformersForClass);
        }
        transformersForClass.add(transformer);
    }

    /**
     * Registers a {@code Transformer} to use when decoding a specific class from BSON. This class will be one of the basic types supported
     * by BSON.
     *
     * @param clazz       the class to be transformed during decoding
     * @param transformer the transformer to use during decoding
     */
    public static void addDecodingHook(final Class<?> clazz, final Transformer transformer) {
        decodeHooks = true;
        List<Transformer> transformersForClass = decodingHooks.get(clazz);
        if (transformersForClass == null) {
            transformersForClass = new CopyOnWriteArrayList<Transformer>();
            decodingHooks.put(clazz, transformersForClass);
        }
        transformersForClass.add(transformer);
    }

    /**
     * Transforms the {@code objectToEncode} using all transformers registered for the class of this object.
     *
     * @param objectToEncode the object being written to BSON.
     * @return the transformed object
     */
    public static Object applyEncodingHooks(final Object objectToEncode) {
        Object transformedObject = objectToEncode;
        if (!hasEncodeHooks() || objectToEncode == null || encodingHooks.size() == 0) {
            return transformedObject;
        }
        List<Transformer> transformersForObject = encodingHooks.get(objectToEncode.getClass());
        if (transformersForObject != null) {
            for (final Transformer transformer : transformersForObject) {
                transformedObject = transformer.transform(objectToEncode);
            }
        }
        return transformedObject;
    }

    /**
     * Transforms the {@code objectToDecode} using all transformers registered for the class of this object.
     *
     * @param objectToDecode the BSON object to decode
     * @return the transformed object
     */
    public static Object applyDecodingHooks(final Object objectToDecode) {
        Object transformedObject = objectToDecode;
        if (!hasDecodeHooks() || objectToDecode == null || decodingHooks.size() == 0) {
            return transformedObject;
        }

        List<Transformer> transformersForObject = decodingHooks.get(objectToDecode.getClass());
        if (transformersForObject != null) {
            for (final Transformer transformer : transformersForObject) {
                transformedObject = transformer.transform(objectToDecode);
            }
        }
        return transformedObject;
    }

    /**
     * Returns the encoding hook(s) associated with the specified class.
     *
     * @param clazz the class to fetch the encoding hooks for
     * @return a List of encoding transformers that apply to the given class
     */
    public static List<Transformer> getEncodingHooks(final Class<?> clazz) {
        return encodingHooks.get(clazz);
    }

    /**
     * Clears <em>all</em> encoding hooks.
     */
    public static void clearEncodingHooks() {
        encodeHooks = false;
        encodingHooks.clear();
    }

    /**
     * Remove all encoding hooks for a specific class.
     *
     * @param clazz the class to remove all the decoding hooks for
     */
    public static void removeEncodingHooks(final Class<?> clazz) {
        encodingHooks.remove(clazz);
    }

    /**
     * Remove a specific encoding hook for a specific class. The {@code transformer} passed as the parameter must be {@code equals} to the
     * transformer to remove.
     *
     * @param clazz       the class to remove the encoding hook for
     * @param transformer the specific encoding hook to remove.
     */
    public static void removeEncodingHook(final Class<?> clazz, final Transformer transformer) {
        getEncodingHooks(clazz).remove(transformer);
    }

    /**
     * Returns the decoding hook(s) associated with the specific class
     *
     * @param clazz the class to fetch the decoding hooks for
     * @return a List of all the decoding Transformers that apply to the given class
     */
    public static List<Transformer> getDecodingHooks(final Class<?> clazz) {
        return decodingHooks.get(clazz);
    }

    /**
     * Clears <em>all</em> decoding hooks.
     */
    public static void clearDecodingHooks() {
        decodeHooks = false;
        decodingHooks.clear();
    }

    /**
     * Remove all decoding hooks for a specific class.
     *
     * @param clazz the class to remove all the decoding hooks for
     */
    public static void removeDecodingHooks(final Class<?> clazz) {
        decodingHooks.remove(clazz);
    }

    /**
     * Remove a specific encoding hook for a specific class.  The {@code transformer} passed as the parameter must be {@code equals} to the
     * transformer to remove.
     *
     * @param clazz       the class to remove the decoding hook for
     * @param transformer the specific decoding hook to remove.
     */
    public static void removeDecodingHook(final Class<?> clazz, final Transformer transformer) {
        getDecodingHooks(clazz).remove(transformer);
    }

    /**
     * Remove all decoding and encoding hooks for all classes.
     */
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
