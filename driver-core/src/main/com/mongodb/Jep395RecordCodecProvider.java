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
package com.mongodb;

import com.mongodb.internal.VisibleForTesting;
import com.mongodb.lang.Nullable;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.record.RecordCodecProvider;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static org.bson.internal.ProvidersCodecRegistry.getFromCodecProvider;


/**
 * A CodecProvider for Java Records.
 * Delegates to {@code org.bson.codecs.record.RecordCodecProvider}.
 * If neither the runtime supports {@code java.lang.Record}, which was introduced in Java SE 17,
 * nor {@code org.bson.codecs.record.RecordCodecProvider} is available,
 * {@linkplain CodecProvider#get(Class, CodecRegistry) provides} {@code null}.
 *
 * @since 4.6
 */
public class Jep395RecordCodecProvider implements CodecProvider {

    @Nullable
    private static final CodecProvider RECORD_CODEC_PROVIDER;
    static {

        CodecProvider possibleCodecProvider;
        try {
            Class.forName("java.lang.Record"); // JEP-395 support canary test.
            Class.forName("org.bson.codecs.record.RecordCodecProvider"); // Java 17 canary test
            possibleCodecProvider = new RecordCodecProvider();
        } catch (ClassNotFoundException | UnsupportedClassVersionError e) {
            // No JEP-395 support
            possibleCodecProvider = null;
        }
        RECORD_CODEC_PROVIDER = possibleCodecProvider;
    }

    @Override
    @Nullable
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        return get(clazz, Collections.emptyList(), registry);
    }

    @Override
    @Nullable
    public <T> Codec<T> get(final Class<T> clazz, final List<Type> typeArguments, final CodecRegistry registry) {
        return RECORD_CODEC_PROVIDER != null ? getFromCodecProvider(RECORD_CODEC_PROVIDER, clazz, typeArguments, registry) : null;
    }

    /**
     * This method is not part of the public API and may be removed or changed at any time.
     *
     * @return true if records are supported
     */
    @VisibleForTesting(otherwise = PRIVATE)
    public boolean hasRecordSupport() {
        return RECORD_CODEC_PROVIDER != null;
    }
}
