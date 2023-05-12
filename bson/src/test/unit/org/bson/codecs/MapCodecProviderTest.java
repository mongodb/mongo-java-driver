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

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MapCodecProviderTest {
    @Test
    void shouldReturnNullForNonMap() {
        MapCodecProvider provider = new MapCodecProvider();
        assertNull(provider.get(String.class, Bson.DEFAULT_CODEC_REGISTRY));
    }

    @Test
    void shouldReturnMapCodecForMap() {
        MapCodecProvider provider = new MapCodecProvider();
        @SuppressWarnings({"rawtypes", "unchecked"})
        Codec<Map<String, Object>> codec = (Codec<Map<String, Object>>) (Codec) provider.get(Map.class, Bson.DEFAULT_CODEC_REGISTRY);
        assertTrue(codec instanceof MapCodecV2);
        MapCodecV2<Map<String, Object>> recordCodec = (MapCodecV2<Map<String, Object>>) codec;
        assertEquals(Map.class, recordCodec.getEncoderClass());
    }

    @Test
    public void shouldReturnMapCodecForMapUsingDefaultRegistry() {
        @SuppressWarnings({"rawtypes", "unchecked"})
        Codec<Map<String, Object>> codec = (Codec<Map<String, Object>>) (Codec) Bson.DEFAULT_CODEC_REGISTRY.get(Map.class);
        assertTrue(codec instanceof MapCodecV2);
        MapCodecV2<Map<String, Object>> recordCodec = (MapCodecV2<Map<String, Object>>) codec;
        assertEquals(Map.class, recordCodec.getEncoderClass());
    }
}
