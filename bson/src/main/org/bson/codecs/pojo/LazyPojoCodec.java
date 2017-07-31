/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.concurrent.ConcurrentMap;

class LazyPojoCodec<T> extends PojoCodec<T> {
    private final ClassModel<T> classModel;
    private final CodecRegistry registry;
    private final DiscriminatorLookup discriminatorLookup;
    private final ConcurrentMap<ClassModel<?>, Codec<?>> codecCache;
    private volatile PojoCodecImpl<T> pojoCodec;

    LazyPojoCodec(final ClassModel<T> classModel, final CodecRegistry registry, final DiscriminatorLookup discriminatorLookup,
                  final ConcurrentMap<ClassModel<?>, Codec<?>> codecCache) {
        this.classModel = classModel;
        this.registry = registry;
        this.discriminatorLookup = discriminatorLookup;
        this.codecCache = codecCache;
    }

    @Override
    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        getPojoCodec().encode(writer, value, encoderContext);
    }

    @Override
    public Class<T> getEncoderClass() {
        return classModel.getType();
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        return getPojoCodec().decode(reader, decoderContext);
    }

    private Codec<T> getPojoCodec() {
        if (pojoCodec == null) {
            pojoCodec = new PojoCodecImpl<T>(classModel, registry, discriminatorLookup, codecCache, true);
        }
        return pojoCodec;
    }

    @Override
    ClassModel<T> getClassModel() {
        return classModel;
    }
}
