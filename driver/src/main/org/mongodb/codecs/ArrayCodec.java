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

package org.mongodb.codecs;

import org.bson.BSONWriter;

public class ArrayCodec implements ComplexTypeEncoder<Object> {
    private final IntegerArrayCodec integerArrayCodec;
    private final LongArrayCodec longArrayCodec;
    private final BooleanArrayCodec booleanArrayCodec;
    private final DoubleArrayCodec doubleArrayCodec;
    private final ByteArrayCodec byteArrayCodec;
    private final FloatArrayCodec floatArrayCodec;
    private final ShortArrayCodec shortArrayCodec;
    private final StringArrayCodec stringArrayCodec;
    private final Codecs codecs;

    public ArrayCodec(final Codecs codecs) {
        this.codecs = codecs;
        integerArrayCodec = new IntegerArrayCodec();
        longArrayCodec = new LongArrayCodec();
        booleanArrayCodec = new BooleanArrayCodec();
        doubleArrayCodec = new DoubleArrayCodec();
        byteArrayCodec = new ByteArrayCodec();
        floatArrayCodec = new FloatArrayCodec();
        shortArrayCodec = new ShortArrayCodec();
        stringArrayCodec = new StringArrayCodec();
    }

    public void encode(final BSONWriter bsonWriter, final Object object) {
        if (object instanceof int[]) {
            encode(bsonWriter, (int[]) object);
        } else if (object instanceof long[]) {
            encode(bsonWriter, (long[]) object);
        } else if (object instanceof float[]) {
            encode(bsonWriter, (float[]) object);
        } else if (object instanceof short[]) {
            encode(bsonWriter, (short[]) object);
        } else if (object instanceof byte[]) {
            encode(bsonWriter, (byte[]) object);
        } else if (object instanceof double[]) {
            encode(bsonWriter, (double[]) object);
        } else if (object instanceof boolean[]) {
            encode(bsonWriter, (boolean[]) object);
        } else if (object instanceof String[]) {
            encode(bsonWriter, (String[]) object);
        } else if (object instanceof Object[]) {
            encode(bsonWriter, (Object[]) object);
        } else {
            System.out.println("AARRGGHH");
        }
    }

    //TODO: maybe the codec needs to be an object codec specifically?
    public void encode(final BSONWriter bsonWriter, final Object[] array) {
        bsonWriter.writeStartArray();
        for (final Object value : array) {
            codecs.encode(bsonWriter, value);
        }
        bsonWriter.writeEndArray();
    }

    public void encode(final BSONWriter bsonWriter, final int[] array) {
        integerArrayCodec.encode(bsonWriter, array);
    }

    public void encode(final BSONWriter bsonWriter, final long[] array) {
        longArrayCodec.encode(bsonWriter, array);
    }

    public void encode(final BSONWriter bsonWriter, final boolean[] array) {
        booleanArrayCodec.encode(bsonWriter, array);
    }

    public void encode(final BSONWriter bsonWriter, final double[] array) {
        doubleArrayCodec.encode(bsonWriter, array);
    }

    public void encode(final BSONWriter bsonWriter, final byte[] array) {
        byteArrayCodec.encode(bsonWriter, array);
    }

    public void encode(final BSONWriter bsonWriter, final float[] array) {
        floatArrayCodec.encode(bsonWriter, array);
    }

    public void encode(final BSONWriter bsonWriter, final short[] array) {
        shortArrayCodec.encode(bsonWriter, array);
    }

    public void encode(final BSONWriter bsonWriter, final String[] array) {
        stringArrayCodec.encode(bsonWriter, array);
    }

}
