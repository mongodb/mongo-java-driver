/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.bson.codecs;

import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

/**
 * A codec provider for classes that extend {@link Enum}.
 *
 * @since 4.5
 */
public final class EnumCodecProvider implements CodecProvider {
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (Enum.class.isAssignableFrom(clazz)) {
            return (Codec<T>) new EnumCodec(clazz);
        }
        return null;
    }
}
