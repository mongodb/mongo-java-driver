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

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.pojo.entities.SimpleEnum;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class EnumCodecProviderTest {
    @Test
    public void shouldProvideCodecForEnum() {
        EnumCodecProvider provider = new EnumCodecProvider();
        Codec<SimpleEnum> codec = provider.get(SimpleEnum.class, CodecRegistries.fromProviders(provider));
        assertNotNull(codec);
        assertEquals(EnumCodec.class, codec.getClass());
    }

    @Test
    public void shouldNotProvideCodecForNonEnum() {
        EnumCodecProvider provider = new EnumCodecProvider();
        Codec<String> codec = provider.get(String.class, CodecRegistries.fromProviders(provider));
        assertNull(codec);
    }
}
