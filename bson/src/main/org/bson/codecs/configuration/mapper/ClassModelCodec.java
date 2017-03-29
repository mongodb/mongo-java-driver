/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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
package org.bson.codecs.configuration.mapper;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

/**
 * Provides the encoding and decoding logic for a ClassModel
 *
 * @param <T> the type to encode/decode
 */
@SuppressWarnings("unchecked")
public class ClassModelCodec<T extends Object> implements Codec<T> {
    private final ClassModel classModel;

    /**
     * Creates a Codec for the ClassModel
     *
     * @param model the model to use
     */
    public ClassModelCodec(final ClassModel model) {
        this.classModel = model;
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        try {
            final T entity = (T) classModel.getType().newInstance();
            reader.readStartDocument();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                final String name = reader.readName();
                final FieldModel fieldModel = classModel.getField(name);
                fieldModel.set(entity, fieldModel.getCodec().decode(reader, decoderContext));

            }
            reader.readEndDocument();
            return entity;
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void encode(final BsonWriter writer, final Object value, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        for (final FieldModel fieldModel : classModel.getFields()) {
            writer.writeName(fieldModel.getName());
            final Codec<Object> codec = fieldModel.getCodec();
            codec.encode(writer, fieldModel.get(value), encoderContext);
        }
        writer.writeEndDocument();
    }

    @Override
    public Class<T> getEncoderClass() {
        return (Class<T>) classModel.getType();
    }
}
