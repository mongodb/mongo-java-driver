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

/**
 * A variant of {@link org.bson.codecs.configuration.CodecProvider} that generates codecs for {@link PojoCodec}.
 *
 * <p>This is a specialized codec provider that retrieves codecs which account for type parameters associated with
 * a property. In particular this should only be used to add support for custom container types like optionals.
 * It's only applicable for use by {@link PojoCodec} registered through {@link PojoCodecProvider#builder()}.
 *
 * @since 3.6
 */
public interface PropertyCodecProvider {

    /**
     * Get a {@code Codec} using the given context, which includes, most importantly, the class and bound type parameters
     * for which a {@code Codec} is required.
     *
     * @param type the class and bound type parameters for which to get a Codec
     * @param registry the registry to use for resolving dependent Codec instances
     * @param <T> the type of the class for which a Codec is required
     * @return the Codec instance, which may be null, if this source is unable to provide one for the requested Class
     */
    <T> Codec<T> get(TypeWithTypeParameters<T> type, PropertyCodecRegistry registry);
}
