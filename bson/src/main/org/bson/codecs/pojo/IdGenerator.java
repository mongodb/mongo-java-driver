/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.bson.codecs.pojo;

/**
 * Classes that implement this interface define a way to create Ids for Pojo's.
 *
 * @param <T> the type of the id value.
 * @since 3.10
 */
public interface IdGenerator<T> {
    /**
     * Generates an id for a Pojo.
     *
     * @return the generated id value
     */
    T generate();

    /**
     * @return the type of the generated id.
     */
    Class<T> getType();
}
