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
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Objects;

/**
 * A {@link Bson} that allows constructing new instances via {@link #newAppended(String, BsonValue)} instead of mutating {@code this}.
 * While instances are not {@link Immutable immutable},
 * {@link BsonDocument#isEmpty() empty} instances are treated specially and are immutable.
 *
 * @param <S> A type defined by the concrete class that extends this abstract class.
 * The presence of this type parameter is an artifact of not having in Java anything resembling
 * the <a href="https://doc.rust-lang.org/std/keyword.SelfTy.html">{@code Self} keyword</a> in Rust.
 */
public abstract class AbstractConstructibleBson<S extends AbstractConstructibleBson<S>> implements Bson {
    private final BsonDocument doc;

    protected AbstractConstructibleBson(final Bson doc) {
        this.doc = doc.toBsonDocument();
    }

    protected abstract S newSelf(BsonDocument doc);

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
        return doc.isEmpty()
                // do not expose empty `doc` to enforce immutability of empty objects
                ? new BsonDocument()
                : doc;
    }

    /**
     * Creates a new instance that contains all mapping from {@code this} and the specified new mapping.
     *
     * @return A new instance.
     * @see BsonDocument#append(String, BsonValue)
     */
    public S newAppended(final String name, final BsonValue value) {
        return newSelf(doc.clone().append(name, value));
    }

    public static AbstractConstructibleBson<?> of(final Bson doc) {
        return new ConstructibleBson(doc);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AbstractConstructibleBson<?> that = (AbstractConstructibleBson<?>) o;
        return doc.equals(that.doc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(doc);
    }

    @Override
    public String toString() {
        return doc.toString();
    }

    private static final class ConstructibleBson extends AbstractConstructibleBson<ConstructibleBson> {
        private ConstructibleBson(final Bson doc) {
            super(doc);
        }

        @Override
        protected ConstructibleBson newSelf(final BsonDocument doc) {
            return new ConstructibleBson(doc);
        }
    }
}
