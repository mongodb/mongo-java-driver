/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.json;

/**
 * A JSON token.
 */
public class JSONToken {

    private final Object value;
    private final JSONTokenType type;

    public JSONToken(final JSONTokenType type, final Object value) {
        this.value = value;
        this.type = type;
    }

    public Object getValue() {
        return value;
    }

    public <T> T getValue(final Class<T> clazz) {
        if (Long.class == clazz && value instanceof Integer) {
            return clazz.cast(((Integer) value).longValue());
        }

        try {
            return clazz.cast(value);
        } catch (ClassCastException e) {
            throw new IllegalStateException(e);
        }
    }

    public JSONTokenType getType() {
        return type;
    }
}
