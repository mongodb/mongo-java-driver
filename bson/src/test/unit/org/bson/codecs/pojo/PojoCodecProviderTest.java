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

package org.bson.codecs.pojo;

import org.bson.codecs.Codec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.entities.SimpleModel;
import org.junit.Test;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public final class PojoCodecProviderTest extends PojoTestCase {

    @Test
    public void testClassNotFound() {
        PojoCodecProvider provider = PojoCodecProvider.builder().build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());
        Codec<SimpleModel> codec = provider.get(SimpleModel.class, registry);
        assertNull(codec);
    }

    @Test
    public void testPackageLessClasses() {
        PojoCodecProvider provider = PojoCodecProvider.builder().build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());
        Codec<Byte> codec = provider.get(byte.class, registry);
        assertNull(codec);
    }

    @Test
    public void testAutomatic() {
        PojoCodecProvider provider = PojoCodecProvider.builder().automatic(true).build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());
        Codec<SimpleModel> codec = provider.get(SimpleModel.class, registry);
        assertNotNull(codec);
    }

    @Test
    public void testAutomaticNoProperty() {
        PojoCodecProvider provider = PojoCodecProvider.builder().automatic(true).build();
        CodecRegistry registry = fromProviders(provider);
        Codec<Integer> codec = provider.get(Integer.class, registry);
        assertNull(codec);
    }

}
