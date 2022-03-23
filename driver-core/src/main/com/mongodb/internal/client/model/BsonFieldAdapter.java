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
package com.mongodb.internal.client.model;

import com.mongodb.client.model.BsonField;
import com.mongodb.client.model.ToBsonField;
import com.mongodb.lang.NonNull;

/**
 * An adapter of {@link BsonField} to {@link ToBsonField}.
 */
public class BsonFieldAdapter implements ToBsonField {
    private final BsonField field;

    public BsonFieldAdapter(final BsonField field) {
        this.field = field;
    }

    @NonNull
    @Override
    public BsonField toBsonField() {
        return field;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BsonFieldAdapter that = (BsonFieldAdapter) o;
        return field.equals(that.field);
    }

    @Override
    public int hashCode() {
        return field.hashCode();
    }

    @Override
    public String toString() {
        return field.toString();
    }
}
