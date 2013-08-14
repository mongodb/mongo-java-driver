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

import org.bson.BSONReader;
import org.bson.BSONType;
import org.bson.BSONWriter;
import org.bson.types.CodeWithScope;
import org.mongodb.Codec;
import org.mongodb.DBRef;
import org.mongodb.Encoder;
import org.mongodb.codecs.validators.QueryFieldNameValidator;
import org.mongodb.codecs.validators.Validator;

import java.util.Map;

import static java.lang.String.format;

public class Codecs implements Codec<Object> {
    private final PrimitiveCodecs primitiveCodecs;
    private final EncoderRegistry encoderRegistry;
    private final IterableCodec iterableCodec;
    private final ArrayCodec arrayCodec;
    private final MapCodec mapCodec;
    private final DBRefEncoder dbRefEncoder;
    private final CodeWithScopeCodec codeWithScopeCodec;
    private final SimpleDocumentCodec simpleDocumentCodec;
    private final Codec<Object> defaultObjectCodec = new NoCodec();

    public Codecs(final PrimitiveCodecs primitiveCodecs, final EncoderRegistry encoderRegistry) {
        this(primitiveCodecs, new QueryFieldNameValidator(), encoderRegistry);
        //defaulting to the less rigorous, and maybe more common, validation - lets through $, dots etc.
    }

    public Codecs(final PrimitiveCodecs primitiveCodecs,
                  final Validator<String> fieldNameValidator,
                  final EncoderRegistry encoderRegistry) {
        this.primitiveCodecs = primitiveCodecs;
        this.encoderRegistry = encoderRegistry;
        arrayCodec = new ArrayCodec(this);
        iterableCodec = new IterableCodec(this);
        mapCodec = new MapCodec(this, fieldNameValidator);
        dbRefEncoder = new DBRefEncoder(this);
        codeWithScopeCodec = new CodeWithScopeCodec(this);
        simpleDocumentCodec = new SimpleDocumentCodec(this);
    }

    public static Codecs createDefault() {
        return builder().primitiveCodecs(PrimitiveCodecs.createDefault()).build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"}) // going to have some unchecked warnings because of all the casting from Object
    public void encode(final BSONWriter bsonWriter, final Object object) {
        if (object == null || primitiveCodecs.canEncode(object.getClass())) {
            primitiveCodecs.encode(bsonWriter, object);
        }
        else if (encoderRegistry.get(object.getClass()) != null) {
            final Encoder<Object> codec = (Encoder<Object>) encoderRegistry.get(object.getClass());
            codec.encode(bsonWriter, object);
        }
        else if (object.getClass().isArray()) {
            arrayCodec.encode(bsonWriter, object);
        }
        else if (object instanceof Map) {
            encode(bsonWriter, (Map) object);
        }
        else {
            encoderRegistry.getDefaultEncoder().encode(bsonWriter, object);
        }
    }

    @Override
    public Class<Object> getEncoderClass() {
        return Object.class;
    }

    public void encode(final BSONWriter bsonWriter, final Iterable<?> value) {
        iterableCodec.encode(bsonWriter, value);
    }

    public void encode(final BSONWriter bsonWriter, final Map<String, Object> value) {
        mapCodec.encode(bsonWriter, value);
    }

    public void encode(final BSONWriter bsonWriter, final DBRef value) {
        dbRefEncoder.encode(bsonWriter, value);
    }

    public void encode(final BSONWriter bsonWriter, final CodeWithScope value) {
        codeWithScopeCodec.encode(bsonWriter, value);
    }

    public static Builder builder() {
        return new Builder();
    }

    // needs to be a raw codec to support Pojo codec, but feels a bit wrong to do this
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setDefaultObjectCodec(final Codec codec) {
        encoderRegistry.register(Object.class, codec);
    }

    //TODO: don't like this at all.  Feels like if it has a BSON type, it's a primitive
    public Object decode(final BSONReader reader) {
        if (primitiveCodecs.canDecodeNextObject(reader)) {
            return primitiveCodecs.decode(reader);
        } else if (reader.getCurrentBSONType() == BSONType.ARRAY) {
            return iterableCodec.decode(reader);
        } else if (reader.getCurrentBSONType() == BSONType.JAVASCRIPT_WITH_SCOPE) {
            return codeWithScopeCodec.decode(reader);
        } else if (reader.getCurrentBSONType() == BSONType.DOCUMENT) {
            return simpleDocumentCodec.decode(reader);
        } else {
            throw new UnsupportedOperationException(format("The BSON type %s does not have a decoder associated with it.",
                                                           reader.getCurrentBSONType()));
        }
    }

    boolean canEncode(final Object object) {
        return object == null
               || primitiveCodecs.canEncode(object.getClass())
               || object.getClass().isArray()
               || object instanceof Map
               || object instanceof Iterable
               || object instanceof CodeWithScope
               || object instanceof DBRef;
    }

    public boolean canDecode(final Class<?> theClass) {
        return theClass.getClass().isArray()
               || primitiveCodecs.canDecode(theClass)
               || iterableCodec.getEncoderClass().isInstance(theClass)
               || mapCodec.getEncoderClass().isAssignableFrom(theClass)
               || dbRefEncoder.getEncoderClass().isInstance(theClass)
               || codeWithScopeCodec.getEncoderClass().isInstance(theClass)
               || simpleDocumentCodec.getEncoderClass().isInstance(theClass);
    }

    public static class Builder {
        private PrimitiveCodecs primitiveCodecs;

        public Builder primitiveCodecs(final PrimitiveCodecs aPrimitiveCodecs) {
            this.primitiveCodecs = aPrimitiveCodecs;
            return this;
        }

        public Codecs build() {
            return new Codecs(primitiveCodecs, new QueryFieldNameValidator(), new EncoderRegistry());
        }
    }
}
