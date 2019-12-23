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

package org.bson;

import java.util.Map;
import java.util.Set;

/**
 * A key-value map that can be saved to the database.
 */
@SuppressWarnings("rawtypes")
public interface BSONObject {

    /**
     * Sets a name/value pair in this object.
     *
     * @param key Name to set
     * @param v   Corresponding value
     * @return the previous value associated with {@code key}, or {@code null} if there was no mapping for {@code key}. (A
     * {@code null} return can also indicate that the map previously associated {@code null} with {@code key}.)
     */
    Object put(String key, Object v);

    /**
     * Sets all key/value pairs from an object into this object
     *
     * @param o the object
     */
    void putAll(BSONObject o);

    /**
     * Sets all key/value pairs from a map into this object
     *
     * @param m the map
     */
    void putAll(Map m);

    /**
     * Gets a field from this object by a given name.
     *
     * @param key The name of the field fetch
     * @return The field, if found
     */
    Object get(String key);

    /**
     * Returns a map representing this BSONObject.
     *
     * @return the map
     */
    Map toMap();

    /**
     * Removes a field with a given name from this object.
     *
     * @param key The name of the field to remove
     * @return The value removed from this object
     */
    Object removeField(String key);

    /**
     * Checks if this object contains a field with the given name.
     *
     * @param s Field name for which to check
     * @return True if the field is present
     */
    boolean containsField(String s);

    /**
     * Returns this object's fields' names
     *
     * @return The names of the fields in this object
     */
    Set<String> keySet();
}

