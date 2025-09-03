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
package com.mongodb.internal.inject;

import com.mongodb.annotations.ThreadSafe;

import java.util.Optional;

/**
 * If a constructor parameter is of type {@link OptionalProvider}, then the corresponding argument must not be {@code null}.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 *
 * @param <T> The type of provided objects.
 * @see Provider
 */
@ThreadSafe
public interface OptionalProvider<T> {
    /**
     * Provides either a fully constructed and injected object or an {@linkplain Optional#isPresent() empty} {@link Optional}
     * to signify that the provider does not provide an object. This method may be called multiple times and must provide the same object.
     * This method must not be called by a constructor that got this provider as its argument.
     */
    Optional<T> optional();
}
