/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.bson.json;

/**
 * A converter from a BSON value to JSON.
 *
 * @param <T> the value type to convert
 * @since 3.5
 */
public interface Converter<T> {
    /**
     * Convert the given value to JSON using the JSON writer.
     *
     * @param value the value, which may be null depending on the type
     * @param writer the JSON writer
     */
    void convert(T value, StrictJsonWriter writer);
}
