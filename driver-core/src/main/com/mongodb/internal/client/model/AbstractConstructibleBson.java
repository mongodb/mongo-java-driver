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
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Objects;

/**
 * A {@link Bson} that allows constructing new instances via {@link #newAppended(String, Object)} instead of mutating {@code this}.
 * While instances are not {@link Immutable immutable},
 * {@link BsonDocument#isEmpty() empty} instances are treated specially and are immutable,
 * provided that the constructor arguments are not mutated.
 *
 * @param <S> A type introduced by the concrete class that extends this abstract class.
 */
public abstract class AbstractConstructibleBson<S extends AbstractConstructibleBson<S>> implements Bson {
    private static final Document EMPTY_APPENDED = new Document();

    private final Bson base;
    private final Document appended;

    protected AbstractConstructibleBson(final Bson base) {
        this(base, EMPTY_APPENDED);
    }

    protected AbstractConstructibleBson(final Bson base, final Document appended) {
        this.base = base;
        this.appended = appended;
    }

    protected abstract S newSelf(Bson base, Document appended);

    @Override
    public final <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        BsonDocument baseDoc = base.toBsonDocument(documentClass, codecRegistry);
        return baseDoc.isEmpty() && appended.isEmpty()
                // eliminate the possibility of exposing internal state when it is empty to enforce immutability of empty objects
                ? new BsonDocument()
                : appended.isEmpty() ? baseDoc : newAppended(baseDoc, appended.toBsonDocument(documentClass, codecRegistry));
    }

    /**
     * Creates a new instance that contains all mapping from {@code this} and the specified new mapping.
     *
     * @return A new instance.
     * @see BsonDocument#append(String, BsonValue)
     */
    protected final S newAppended(final String name, final Object value) {
        return newSelf(base, new Document(appended).append(name, value));
    }

    public static AbstractConstructibleBson<?> of(final Bson doc) {
        return new ConstructibleBson(doc);
    }

    @Override
    public final boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AbstractConstructibleBson<?> that = (AbstractConstructibleBson<?>) o;
        return Objects.equals(base, that.base) && Objects.equals(appended, that.appended);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(base, appended);
    }

    @Override
    public String toString() {
        return "{base=" + base
                + ", appended=" + appended
                + '}';
    }

    private static BsonDocument newAppended(final BsonDocument base, final BsonDocument appended) {
        final BsonDocument result = base.clone();
        result.putAll(appended);
        return result;
    }

    private static final class ConstructibleBson extends AbstractConstructibleBson<ConstructibleBson> {
        private ConstructibleBson(final Bson base) {
            super(base);
        }

        private ConstructibleBson(final Bson base, final Document appended) {
            super(base, appended);
        }

        @Override
        protected ConstructibleBson newSelf(final Bson base, final Document appended) {
            return new ConstructibleBson(base, appended);
        }
    }
}
