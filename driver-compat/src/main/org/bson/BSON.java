/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

import com.mongodb.DBObject;
import com.mongodb.MongoInternalException;
import com.mongodb.serializers.DBObjectSerializer;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferInput;
import org.bson.io.InputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.util.ClassMap;
import org.mongodb.serialization.PrimitiveSerializers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

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
    public static byte[] encode(final DBObject doc) {
        try {
            final OutputBuffer buffer = new BasicOutputBuffer();
            new DBObjectSerializer(PrimitiveSerializers.createDefault()).serialize(new BSONBinaryWriter(buffer), doc);
            final BufferExposingByteArrayOutputStream stream = new BufferExposingByteArrayOutputStream();
            buffer.pipe(stream);
            return stream.getInternalBytes();
        } catch (IOException e) {
            // impossible with a byte array output stream
            throw new MongoInternalException("impossible", e);
        }
    }

    /**
     * Decodes a BSON byte array into a DBObject instance.
     *
     * @param bytes a document encoded as BSON
     * @return the document as a DBObject
     */
    public static DBObject decode(final byte[] bytes) {
        final InputBuffer buffer = new ByteBufferInput(ByteBuffer.wrap(bytes));
        return new DBObjectSerializer(PrimitiveSerializers.createDefault()).deserialize(new BSONBinaryReader(buffer));
    }

    // Just so we don't have to copy the buffer
    static class BufferExposingByteArrayOutputStream extends ByteArrayOutputStream {
        BufferExposingByteArrayOutputStream() {
            super(512);
        }

        byte[] getInternalBytes() {
            return buf;
        }
    }
}
