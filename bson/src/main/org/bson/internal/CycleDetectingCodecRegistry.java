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

package org.bson.internal;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;

/**
 * A marker interface for {@code CodecRegistry} implementations that are able to detect cycles.
 *
 * @since 3.12
 */
interface CycleDetectingCodecRegistry extends CodecRegistry {
    /**
     * Get the Codec using the given context.
     *
     * @param context the child context
     * @param <T> the value type
     * @return the Codec
     */
    <T> Codec<T> get(ChildCodecRegistry<T> context);
}
