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

import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import java.util.Set;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CollectionCodecProviderTest {
    @Test
    void shouldReturnNullForNonCollection() {
        CollectionCodecProvider provider = new CollectionCodecProvider();
        assertNull(provider.get(String.class, Bson.DEFAULT_CODEC_REGISTRY));
    }

    @Test
    void shouldReturnCollectionCodecForCollection() {
        CollectionCodecProvider provider = new CollectionCodecProvider();
        @SuppressWarnings({"rawtypes", "unchecked"})
        Codec<Set<Object>> codec = (Codec<Set<Object>>) (Codec) provider.get(Set.class, Bson.DEFAULT_CODEC_REGISTRY);
        assertTrue(codec instanceof CollectionCodec);
        CollectionCodec<Set<Object>> recordCodec = (CollectionCodec<Set<Object>>) codec;
        assertEquals(Set.class, recordCodec.getEncoderClass());
    }

    @Test
    public void shouldReturnCollectionCodecForCollectionUsingDefaultRegistry() {
        @SuppressWarnings({"rawtypes", "unchecked"})
        Codec<Set<Object>> codec = (Codec<Set<Object>>) (Codec) Bson.DEFAULT_CODEC_REGISTRY.get(Set.class);
        assertTrue(codec instanceof CollectionCodec);
        CollectionCodec<Set<Object>> recordCodec = (CollectionCodec<Set<Object>>) codec;
        assertEquals(Set.class, recordCodec.getEncoderClass());
    }
}
