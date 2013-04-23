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
import org.mongodb.Codec;
import org.mongodb.DBRef;
import org.mongodb.codecs.validators.QueryFieldNameValidator;
import org.mongodb.codecs.validators.Validator;

import java.util.Map;

public class Codecs {
    private final PrimitiveCodecs primitiveCodecs;
    private final IterableCodec iterableCodec;
    private final ArrayCodec arrayCodec;
    private final MapCodec mapCodec;
    private final DBRefCodec dbRefCodec;
    private Codec<Object> defaultObjectCodec = new NoOpCodec();

    public Codecs(final PrimitiveCodecs primitiveCodecs) {
        this(primitiveCodecs, new QueryFieldNameValidator());
        //defaulting to the less rigorous, and maybe more common, validation - lets through $, dots etc.
    }

    public Codecs(final PrimitiveCodecs primitiveCodecs, final Validator<String> fieldNameValidator) {
        this.primitiveCodecs = primitiveCodecs;
        arrayCodec = new ArrayCodec(this);
        iterableCodec = new IterableCodec(this);
        mapCodec = new MapCodec(this, fieldNameValidator);
        dbRefCodec = new DBRefCodec(this);
    }

    public static Codecs createDefault() {
        return builder().primitiveCodecs(PrimitiveCodecs.createDefault()).build();
    }

    @SuppressWarnings("unchecked") // going to have some unchecked warnings because of all the casting from Object
    public void encode(final BSONWriter bsonWriter, final Object object) {
        if (isBSONPrimitive(object)) {
            primitiveCodecs.encode(bsonWriter, object);
        } else if (object.getClass().isArray()) {
            arrayCodec.encode(bsonWriter, object);
        } else if (object instanceof Map) {
            encode(bsonWriter, (Map) object);
        } else if (object instanceof Iterable) {
            encode(bsonWriter, (Iterable) object);
        } else {
            defaultObjectCodec.encode(bsonWriter, object);
            //throw new RuntimeException("AARRGGHH!  I have no idea what to do with a " + object.getClass());
            //            System.out.println(Iterable.class.isAssignableFrom(value.getClass()));
            //            final Codec codec = classToCodeMap.get(value.getClass());
            //            if (Iterable.class.isAssignableFrom(value.getClass()))
            //            codec.encode(bsonWriter, value);
        }
    }

    public void encode(final BSONWriter bsonWriter, final Iterable<?> value) {
        iterableCodec.encode(bsonWriter, value);
    }

    public void encode(final BSONWriter bsonWriter, final Map<String, Object> value) {
        mapCodec.encode(bsonWriter, value);
    }

    public void encode(final BSONWriter bsonWriter, final DBRef value) {
        dbRefCodec.encode(bsonWriter, value);
    }

    private boolean isBSONPrimitive(final Object value) {
        return primitiveCodecs.canEncode(value.getClass());
    }

    public static Builder builder() {
        return new Builder();
    }

    public void setDefaultObjectCodec(final Codec<Object> codec) {
        this.defaultObjectCodec = codec;
    }

    public static class Builder {
        private PrimitiveCodecs primitiveCodecs;

        public Builder primitiveCodecs(final PrimitiveCodecs aPrimitiveCodecs) {
            this.primitiveCodecs = aPrimitiveCodecs;
            return this;
        }

        public Codecs build() {
            return new Codecs(primitiveCodecs, new QueryFieldNameValidator());
        }
    }
}
