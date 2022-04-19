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

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;


/**
 * A CodecProvider for Java Records.
 *
 * <p>Requires java.lang.Record support - eg Java 17 or greater.</p>
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
            possibleCodecProvider = new RecordCodecProvider();
        } catch (ClassNotFoundException e) {
            // No JEP-395 support
            possibleCodecProvider = null;
        }
        RECORD_CODEC_PROVIDER = possibleCodecProvider;
    }

    @Override
    @Nullable
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        return RECORD_CODEC_PROVIDER != null ? RECORD_CODEC_PROVIDER.get(clazz, registry) : null;
    }

    /**
     * @return true if records are supported
     */
    @VisibleForTesting(otherwise = PRIVATE)
    public boolean hasRecordSupport() {
        return RECORD_CODEC_PROVIDER != null;
    }
}

