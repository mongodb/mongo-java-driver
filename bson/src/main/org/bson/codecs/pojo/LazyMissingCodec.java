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
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;


class LazyMissingCodec<S> implements Codec<S> {
    private final Class<S> clazz;
    private final CodecConfigurationException exception;

    LazyMissingCodec(final Class<S> clazz, final CodecConfigurationException exception) {
        this.clazz = clazz;
        this.exception = exception;
    }

    @Override
    public S decode(final BsonReader reader, final DecoderContext decoderContext) {
        throw exception;
    }

    @Override
    public void encode(final BsonWriter writer, final S value, final EncoderContext encoderContext) {
        throw exception;
    }

    @Override
    public Class<S> getEncoderClass() {
        return clazz;
    }
}
