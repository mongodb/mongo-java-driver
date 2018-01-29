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

/**
 * An interface allowing a {@link PropertyModel} to determine if a value should be serialized.
 *
 * @param <T> the type of the property.
 * @since 3.5
 */
public interface PropertySerialization<T> {

    /**
     * Determines if a value should be serialized
     *
     * @param value the value to check
     * @return true if the value should be serialized
     */
    boolean shouldSerialize(T value);
}
