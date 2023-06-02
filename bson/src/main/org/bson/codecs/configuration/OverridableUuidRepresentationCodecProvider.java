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

package org.bson.codecs.configuration;

import org.bson.UuidRepresentation;
import org.bson.codecs.Codec;
import org.bson.codecs.OverridableUuidRepresentationCodec;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;

import static org.bson.assertions.Assertions.notNull;
import static org.bson.internal.ProvidersCodecRegistry.getFromCodecProvider;

final class OverridableUuidRepresentationCodecProvider implements CodecProvider {

    private final CodecProvider wrapped;
    private final UuidRepresentation uuidRepresentation;

    OverridableUuidRepresentationCodecProvider(final CodecProvider wrapped, final UuidRepresentation uuidRepresentation) {
        this.uuidRepresentation = notNull("uuidRepresentation", uuidRepresentation);
        this.wrapped = notNull("wrapped", wrapped);
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        return get(clazz, Collections.emptyList(), registry);
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz, final List<Type> typeArguments, final CodecRegistry registry) {
        Codec<T> codec = getFromCodecProvider(wrapped, clazz, typeArguments, registry);
        if (codec instanceof OverridableUuidRepresentationCodec) {
            @SuppressWarnings("unchecked")
            Codec<T> codecWithUuidRepresentation = ((OverridableUuidRepresentationCodec<T>) codec).withUuidRepresentation(uuidRepresentation);
            codec = codecWithUuidRepresentation;
        }
        return codec;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OverridableUuidRepresentationCodecProvider that = (OverridableUuidRepresentationCodecProvider) o;

        if (!wrapped.equals(that.wrapped)) {
            return false;
        }
        return uuidRepresentation == that.uuidRepresentation;
    }

    @Override
    public int hashCode() {
        int result = wrapped.hashCode();
        result = 31 * result + uuidRepresentation.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "OverridableUuidRepresentationCodecRegistry{"
                + "wrapped=" + wrapped
                + ", uuidRepresentation=" + uuidRepresentation
                + '}';
    }
}
