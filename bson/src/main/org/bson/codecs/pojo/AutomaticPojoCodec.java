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

package org.bson.codecs.pojo;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;

import static java.lang.String.format;

final class AutomaticPojoCodec<T> extends PojoCodec<T> {
    private final PojoCodec<T> pojoCodec;

    AutomaticPojoCodec(final PojoCodec<T> pojoCodec) {
        this.pojoCodec = pojoCodec;
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        try {
            return pojoCodec.decode(reader, decoderContext);
        } catch (CodecConfigurationException e) {
            throw new CodecConfigurationException(
                    format("An exception occurred when decoding using the AutomaticPojoCodec.%n"
                            + "Decoding into a '%s' failed with the following exception:%n%n%s%n%n"
                            + "A custom Codec or PojoCodec may need to be explicitly configured and registered to handle this type.",
                            pojoCodec.getEncoderClass().getSimpleName(), e.getMessage()), e);
        }
    }

    @Override
    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        try {
            pojoCodec.encode(writer, value, encoderContext);
        } catch (CodecConfigurationException e) {
            throw new CodecConfigurationException(
                    format("An exception occurred when encoding using the AutomaticPojoCodec.%n"
                            + "Encoding a %s: '%s' failed with the following exception:%n%n%s%n%n"
                            + "A custom Codec or PojoCodec may need to be explicitly configured and registered to handle this type.",
                            getEncoderClass().getSimpleName(), value, e.getMessage()), e);
        }
    }

    @Override
    public Class<T> getEncoderClass() {
        return pojoCodec.getEncoderClass();
    }

    @Override
    ClassModel<T> getClassModel() {
        return pojoCodec.getClassModel();
    }
}
