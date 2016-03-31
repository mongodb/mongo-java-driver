/*
 * Copyright 2008-2016 MongoDB, Inc.
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

package org.bson.json;

import org.bson.BsonDouble;
import org.bson.types.Decimal128;

/**
 * A JSON token.
 */
class JsonToken {

    private final Object value;
    private final JsonTokenType type;

    public JsonToken(final JsonTokenType type, final Object value) {
        this.value = value;
        this.type = type;
    }

    public Object getValue() {
        return value;
    }

    public <T> T getValue(final Class<T> clazz) {
        if (Long.class == clazz) {
            if (value instanceof Integer) {
                return clazz.cast(((Integer) value).longValue());
            } else if (value instanceof String) {
                return clazz.cast(Long.valueOf((String) value));
            }
        } else if (Decimal128.class == clazz) {
            if (value instanceof Integer) {
                return clazz.cast(new Decimal128((Integer) value));
            } else if (value instanceof Long) {
                return clazz.cast(new Decimal128((Long) value));
            } else if (value instanceof Double) {
                return clazz.cast(new BsonDouble((Double) value).decimal128Value());
            } else if (value instanceof String) {
                return clazz.cast(Decimal128.parse((String) value));
            }
        }

        try {
            return clazz.cast(value);
        } catch (ClassCastException e) {
            throw new IllegalStateException(e);
        }
    }

    public JsonTokenType getType() {
        return type;
    }
}
