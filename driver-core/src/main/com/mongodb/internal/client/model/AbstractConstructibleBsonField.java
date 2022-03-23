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

import com.mongodb.annotations.Immutable;
import com.mongodb.client.model.BsonField;
import com.mongodb.client.model.ToBsonField;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.Objects;

/**
 * A {@link ToBsonField} that allows constructing new instances via {@link #newWithAppendedValue(String, BsonValue)}
 * instead of mutating {@code this}.
 * While instances are not {@link Immutable immutable},
 * instances with {@link BsonDocument#isEmpty() empty} {@linkplain BsonField#getValue() values} are treated specially and are immutable.
 *
 * @param <S> A type defined by the concrete class that extends this abstract class.
 * @see AbstractConstructibleBson
 */
public abstract class AbstractConstructibleBsonField<S extends AbstractConstructibleBsonField<S>> implements ToBsonField {
    private final String name;
    private final AbstractConstructibleBson<?> value;

    protected AbstractConstructibleBsonField(final String name, final Bson value) {
        this.name = name;
        this.value = AbstractConstructibleBson.of(value.toBsonDocument());
    }

    protected abstract S newSelf(String name, Bson value);

    /**
     * Creates a new instance with a {@linkplain BsonField#getValue() value} that contains all mapping from {@code this} value
     * and the specified new mapping.
     *
     * @return A new instance.
     * @see AbstractConstructibleBson#newAppended(String, BsonValue)
     */
    public S newWithAppendedValue(final String name, final BsonValue value) {
        return newSelf(this.name, this.value.newAppended(name, value));
    }

    @Override
    public BsonField toBsonField() {
        return new BsonField(name, value.toBsonDocument());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AbstractConstructibleBsonField<?> that = (AbstractConstructibleBsonField<?>) o;
        return name.equals(that.name) && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }
}
