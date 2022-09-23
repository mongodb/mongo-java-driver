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

import java.lang.reflect.Type;
import java.util.List;

/**
 * An interface indicating that a Codec is for a type that can be parameterized by generic types.
 *
 * @since 4.8
 */
public interface Parameterizable {
    /**
     * Recursively parameterize the codec with the given registry and generic type arguments.
     *
     * @param codecRegistry the code registry to use to resolve codecs for the generic type arguments
     * @param types the types that are parameterizing the containing type.
     * @return the Codec parameterized with the given types
     */
    Codec<?> parameterize(CodecRegistry codecRegistry, List<Type> types);
}
