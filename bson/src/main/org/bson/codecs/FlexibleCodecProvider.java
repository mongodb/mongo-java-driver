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

package org.bson.codecs;

import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@code CodecProvider} for all codecs that can read in different values depending on
 * the BsonRepresentation.
 *
 * @since 4.2
 */
public class FlexibleCodecProvider implements CodecProvider {
    private final Map<Class<?>, FlexibleCodec<?>> codecs = new HashMap<Class<?>, FlexibleCodec<?>>();

    /**
     * Construct a new instance of the provider.
     */
    public FlexibleCodecProvider() {
        addCodecs();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> FlexibleCodec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        return (FlexibleCodec<T>) codecs.get(clazz);
    }

    private void addCodecs() {
        addCodec(new FlexibleStringCodec());
    }

    private <T> void addCodec(final FlexibleCodec<T> codec) {
        codecs.put(codec.getEncoderClass(), codec);
    }

}
