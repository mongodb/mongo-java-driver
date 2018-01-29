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
 * Provides access for setting data and the creation of a class instances.
 *
 * @param <T> the type of the class
 * @since 3.5
 */
public interface InstanceCreator<T> {

    /**
     * Sets a value for the given PropertyModel
     *
     * @param value      the new value for the property
     * @param propertyModel the PropertyModel representing the property to set the value for.
     * @param <S>        the PropertyModel's type
     */
    <S> void set(S value, PropertyModel<S> propertyModel);

    /**
     * Returns the new instance of the class.
     * <p>Note: This will be called after all the values have been set.</p>
     *
     * @return the new class instance.
     */
    T getInstance();

}
