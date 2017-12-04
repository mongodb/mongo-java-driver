/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;

final class FallbackPropertyCodecProvider implements PropertyCodecProvider {
    private final CodecRegistry codecRegistry;
    private final PojoCodec<?> pojoCodec;

    FallbackPropertyCodecProvider(final PojoCodec<?> pojoCodec, final CodecRegistry codecRegistry) {
        this.pojoCodec = pojoCodec;
        this.codecRegistry = codecRegistry;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S> Codec<S> get(final TypeWithTypeParameters<S> type, final PropertyCodecRegistry propertyCodecRegistry) {
        Class<S> clazz = type.getType();
        if (clazz == pojoCodec.getEncoderClass()) {
            return (Codec<S>) pojoCodec;
        }
        return codecRegistry.get(type.getType());
    }
}
