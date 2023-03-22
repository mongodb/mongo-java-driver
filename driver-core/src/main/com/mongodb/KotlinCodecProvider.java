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

import com.mongodb.lang.Nullable;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.kotlin.DataClassCodecProvider;
import org.bson.codecs.kotlinx.KotlinSerializerCodecProvider;

/**
 * A CodecProvider for Kotlin data classes.
 *
 * <p>Requires the bson-kotlin package and / or the bson-kotlinx package.</p>
 *
 * @since 4.10
 */
public class KotlinCodecProvider implements CodecProvider {

    @Nullable
    private static final CodecProvider KOTLIN_SERIALIZABLE_CODEC_PROVIDER;
    @Nullable
    private static final CodecProvider DATA_CLASS_CODEC_PROVIDER;

    static {
        CodecProvider possibleCodecProvider = null;

        try {
            Class.forName("org.bson.codecs.kotlinx.KotlinSerializerCodecProvider"); // Kotlinx bson canary test
            possibleCodecProvider = new KotlinSerializerCodecProvider();
        } catch (ClassNotFoundException e) {
            // No kotlinx support
        }
        KOTLIN_SERIALIZABLE_CODEC_PROVIDER = possibleCodecProvider;

        possibleCodecProvider = null;
        try {
            Class.forName("org.bson.codecs.kotlin.DataClassCodecProvider"); // Kotlin bson canary test
            possibleCodecProvider = new DataClassCodecProvider();
        } catch (ClassNotFoundException e) {
            // No kotlin data class support
        }
        DATA_CLASS_CODEC_PROVIDER = possibleCodecProvider;
    }

    @Override
    @Nullable
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        Codec<T> codec = null;
        if (KOTLIN_SERIALIZABLE_CODEC_PROVIDER != null) {
            codec = KOTLIN_SERIALIZABLE_CODEC_PROVIDER.get(clazz, registry);
        }

        if (codec == null && DATA_CLASS_CODEC_PROVIDER != null) {
            codec = DATA_CLASS_CODEC_PROVIDER.get(clazz, registry);
        }
        return codec;
    }

}

