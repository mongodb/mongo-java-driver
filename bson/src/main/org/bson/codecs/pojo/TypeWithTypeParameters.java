/*
 * Copyright 2017 MongoDB, Inc.
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

import java.util.List;

/**
 * A combination of a type and its type parameters.
 *
 * @param <T> the type which potentially has parameterized types
 * @since 3.6
 */
public interface TypeWithTypeParameters<T> {
    /**
     * @return the class this {@code TypeWithTypeParameters} represents
     */
    Class<T> getType();

    /**
     * @return the type parameters for {@link #getType()}
     */
    List<? extends TypeWithTypeParameters<?>> getTypeParameters();
}
