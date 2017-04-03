/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo;

/**
 * Provides access for getting and setting field data.
 *
 * @param <T> the type of the field
 * @since 3.5
 */
public interface FieldAccessor<T> {

    /**
     * Gets the value for a given FieldModel instance.
     *
     * @param instance the class instance to get the field value from
     * @param <S>      the class instance type
     * @return the value of the field.
     */
    <S> T get(S instance);

    /**
     * Sets a value on the given FieldModel
     *
     * @param instance the instance to set the field value to
     * @param <S>      the class instance type
     * @param value    the new value for the field
     */
    <S> void set(S instance, T value);
}
