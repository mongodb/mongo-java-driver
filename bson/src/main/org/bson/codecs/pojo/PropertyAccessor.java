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
 * Provides access for getting and setting property data.
 *
 * @param <T> the type of the property
 * @since 3.5
 */
public interface PropertyAccessor<T> {

    /**
     * Gets the value for a given PropertyModel instance.
     *
     * @param instance the class instance to get the property value from
     * @param <S>      the class instance type
     * @return the value of the property.
     */
    <S> T get(S instance);

    /**
     * Sets a value on the given PropertyModel
     *
     * @param instance the instance to set the property value to
     * @param <S>      the class instance type
     * @param value    the new value for the property
     */
    <S> void set(S instance, T value);
}
