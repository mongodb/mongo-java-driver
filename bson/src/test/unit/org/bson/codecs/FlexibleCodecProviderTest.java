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

import org.bson.codecs.configuration.CodecRegistry;
import org.junit.Test;

import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class FlexibleCodecProviderTest {
    private final FlexibleCodecProvider provider = new FlexibleCodecProvider();
    private final CodecRegistry registry = fromCodecs();

    @Test
    public void testProvidesSupportedCodecs() {
        assertEquals(provider.get(String.class, registry).getClass(), FlexibleStringCodec.class);
        assertNull(provider.get(Integer.class, registry));
    }
}
