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
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * A {@link Bson} that contains exactly one name/value pair
 * and allows constructing new instances via {@link #newWithAppendedValue(String, Object)} instead of mutating {@code this}.
 * The value must itself be a {@code Bson}.
 * While instances are not {@link Immutable immutable},
 * instances with {@link BsonDocument#isEmpty() empty} values are treated specially and are immutable,
 * provided that the constructor arguments are not mutated.
 *
 * @param <S> A type introduced by the concrete class that extends this abstract class.
 * @see AbstractConstructibleBson
 */
public abstract class AbstractConstructibleBsonElement<S extends AbstractConstructibleBsonElement<S>> implements Bson {
    private final String name;
    private final AbstractConstructibleBson<?> value;

    protected AbstractConstructibleBsonElement(final String name, final Bson value) {
        this.name = name;
        this.value = value instanceof AbstractConstructibleBson
                // prevent double wrapping
                ? (AbstractConstructibleBson<?>) value
                : AbstractConstructibleBson.of(value);
    }

    protected abstract S newSelf(String name, Bson value);

    /**
     * {@linkplain Document#append(String, Object) Appends} the specified mapping to the value via {@link #newWithMutatedValue(Consumer)}.
     *
     * @return A new instance.
     */
    protected final S newWithAppendedValue(final String name, final Object value) {
        return newWithMutatedValue(doc -> doc.append(name, value));
    }

    /**
     * Creates a copy of {@code this} with a value that is
     * a {@linkplain AbstractConstructibleBson#newMutated(Consumer) shallow copy} of this value mutated via the specified {@code mutator}.
     *
     * @return A new instance.
     * @see AbstractConstructibleBson#newMutated(Consumer)
     */
    protected final S newWithMutatedValue(final Consumer<Document> mutator) {
        return newSelf(this.name, this.value.newMutated(mutator));
    }

    @Override
    public final <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        return new BsonDocument(name, value.toBsonDocument(documentClass, codecRegistry));
    }

    public static AbstractConstructibleBsonElement<?> of(final String name, final Bson value) {
        return new ConstructibleBsonElement(name, value);
    }

    @Override
    public final boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AbstractConstructibleBsonElement<?> that = (AbstractConstructibleBsonElement<?>) o;
        return name.equals(that.name) && value.equals(that.value);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public String toString() {
        return "{\""
                + name + "\": " + value.toString()
                + '}';
    }

    private static final class ConstructibleBsonElement extends AbstractConstructibleBsonElement<ConstructibleBsonElement> {
        private ConstructibleBsonElement(final String name, final Bson value) {
            super(name, value);
        }

        @Override
        protected ConstructibleBsonElement newSelf(final String name, final Bson value) {
            return new ConstructibleBsonElement(name, value);
        }
    }
}
