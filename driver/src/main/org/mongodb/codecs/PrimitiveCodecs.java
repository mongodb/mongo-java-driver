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

package org.mongodb.codecs;

import org.bson.BSONReader;
import org.bson.BSONType;
import org.bson.BSONWriter;
import org.mongodb.Codec;
import org.mongodb.Decoder;
import org.mongodb.Encoder;
import org.mongodb.MongoException;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// TODO: this is getting better, but still not sure

/**
 * Holder for all the codec mappings.
 */
// Suspect that the rawtypes warnings are telling us something that we haven't done cleanly
// we should address these
@SuppressWarnings("rawtypes")
//CHECKSTYLE:OFF
public class PrimitiveCodecs implements Codec<Object> {
    //CHECKSTYLE:ON
    private Map<Class, Encoder<?>> classEncoderMap = new HashMap<Class, Encoder<?>>();
    private Map<BSONType, Decoder<?>> bsonTypeDecoderMap = new EnumMap<BSONType, Decoder<?>>(BSONType.class);
    private Set<Class> supportedDecodeTypes = new HashSet<Class>();

    protected PrimitiveCodecs(final Map<Class, Encoder<?>> classEncoderMap,
                              final Map<BSONType, Decoder<?>> bsonTypeDecoderMap,
                              final Set<Class> supportedDecodeTypes) {
        this.classEncoderMap = classEncoderMap;
        this.bsonTypeDecoderMap = bsonTypeDecoderMap;
        this.supportedDecodeTypes = supportedDecodeTypes;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void encode(final BSONWriter writer, final Object value) {
        Encoder codec;
        if (value == null) {
            codec = classEncoderMap.get(null);
        } else {
            codec = classEncoderMap.get(value.getClass());
        }
        if (codec == null) {
            throw new EncodingException("No codec for class " + (value != null ? value.getClass().getName() : "with null value"));
        }
        codec.encode(writer, value);  // TODO: unchecked call
    }

    @Override
    public Object decode(final BSONReader reader) {
        BSONType bsonType = reader.getCurrentBSONType();
        Decoder codec = bsonTypeDecoderMap.get(bsonType);
        if (codec == null) {
            throw new MongoException("Unable to find decoder for BSON type " + bsonType);
        }
        return codec.decode(reader);
    }

    @Override
    public Class<Object> getEncoderClass() {
        return Object.class;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(final PrimitiveCodecs base) {
        return new Builder(base);
    }

    // TODO: find a proper way to do this...
    public static PrimitiveCodecs createDefault() {
        return builder()
               .objectIdCodec(new ObjectIdCodec())
               .integerCodec(new IntegerCodec())
               .longCodec(new LongCodec())
               .stringCodec(new StringCodec())
               .doubleCodec(new DoubleCodec())
               .dateCodec(new DateCodec())
               .timestampCodec(new TimestampCodec())
               .booleanCodec(new BooleanCodec())
               .patternCodec(new PatternCodec())
               .minKeyCodec(new MinKeyCodec())
               .maxKeyCodec(new MaxKeyCodec())
               .javascriptCodec(new CodeCodec())
               .nullCodec(new NullCodec())
               .otherEncoder(new FloatCodec())
               .otherEncoder(new ShortCodec())
               .otherEncoder(new ByteCodec())
               .otherEncoder(new ByteArrayCodec())
               .otherEncoder(new BinaryEncoder())
               .otherEncoder(new UUIDEncoder())
               .otherDecoder(BSONType.DB_POINTER, new DBPointerDecoder())
               .binaryDecoder(new TransformingBinaryDecoder())
               .build();
    }

    boolean canEncode(final Class<?> aClass) {
        return classEncoderMap.containsKey(aClass);
    }

    boolean canDecodeNextObject(final BSONReader reader) {
        return bsonTypeDecoderMap.containsKey(reader.getCurrentBSONType());
    }

    boolean canDecode(final Class theClass) {
        return supportedDecodeTypes.contains(theClass);
    }

    public static class Builder {
        private final Map<Class, Encoder<?>> classEncoderMap = new HashMap<Class, Encoder<?>>();
        private final Map<BSONType, Decoder<?>> bsonTypeDecoderMap = new HashMap<BSONType, Decoder<?>>();
        private final Set<Class> supportedDecodeTypes = new HashSet<Class>();

        public Builder() {
        }

        public Builder(final PrimitiveCodecs base) {
            classEncoderMap.putAll(base.classEncoderMap);
            bsonTypeDecoderMap.putAll(base.bsonTypeDecoderMap);
            supportedDecodeTypes.addAll(base.supportedDecodeTypes);
        }


        public Builder objectIdCodec(final Codec codec) {
            registerCodec(BSONType.OBJECT_ID, codec);
            return this;
        }

        public Builder integerCodec(final Codec codec) {
            registerCodec(BSONType.INT32, codec);
            return this;
        }

        public Builder longCodec(final Codec codec) {
            registerCodec(BSONType.INT64, codec);
            return this;
        }

        public Builder stringCodec(final Codec codec) {
            registerCodec(BSONType.STRING, codec);
            return this;
        }

        public Builder doubleCodec(final Codec codec) {
            registerCodec(BSONType.DOUBLE, codec);
            return this;
        }

        public Builder binaryDecoder(final Decoder decoder) {
            bsonTypeDecoderMap.put(BSONType.BINARY, decoder);
            return this;
        }

        public Builder dateCodec(final Codec codec) {
            registerCodec(BSONType.DATE_TIME, codec);
            return this;
        }

        public Builder timestampCodec(final Codec codec) {
            registerCodec(BSONType.TIMESTAMP, codec);
            return this;
        }

        public Builder booleanCodec(final Codec codec) {
            registerCodec(BSONType.BOOLEAN, codec);
            return this;
        }

        public Builder patternCodec(final Codec codec) {
            registerCodec(BSONType.REGULAR_EXPRESSION, codec);
            return this;
        }

        public Builder minKeyCodec(final Codec codec) {
            registerCodec(BSONType.MIN_KEY, codec);
            return this;
        }

        public Builder maxKeyCodec(final Codec codec) {
            registerCodec(BSONType.MAX_KEY, codec);
            return this;
        }

        public Builder javascriptCodec(final Codec codec) {
            registerCodec(BSONType.JAVASCRIPT, codec);
            return this;
        }

        public Builder nullCodec(final Codec codec) {
            registerCodec(BSONType.NULL, codec);
            return this;
        }

        /**
         * Used to register encoders for types that are not also decoded to.`
         *
         * @param encoder the encoder
         * @return this
         */
        public Builder otherEncoder(final Encoder encoder) {
            classEncoderMap.put(encoder.getEncoderClass(), encoder);
            return this;
        }

        /**
         * Used to register a decoder for which does not have the same type as the encoder.
         *
         * @param bsonType the bson type to decode
         * @param decoder  the decoder
         * @return this
         */
        public Builder otherDecoder(final BSONType bsonType, final Decoder decoder) {
            bsonTypeDecoderMap.put(bsonType, decoder);
            return this;
        }

        public PrimitiveCodecs build() {
            return new PrimitiveCodecs(classEncoderMap, bsonTypeDecoderMap, supportedDecodeTypes);
        }

        private void registerCodec(final BSONType bsonType, final Codec<?> codec) {
            bsonTypeDecoderMap.put(bsonType, codec);
            classEncoderMap.put(codec.getEncoderClass(), codec);
            supportedDecodeTypes.add(codec.getEncoderClass());
        }
    }
}
