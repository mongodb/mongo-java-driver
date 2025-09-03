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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import static org.bson.internal.BsonUtil.mutableDeepCopy;

/**
 * A {@link Bson} that allows constructing new instances via {@link #newAppended(String, Object)} instead of mutating {@code this}.
 * See {@link #AbstractConstructibleBson(Bson, Document)} for the note on mutability.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 *
 * @param <S> A type introduced by the concrete class that extends this abstract class.
 * @see AbstractConstructibleBsonElement
 */
public abstract class AbstractConstructibleBson<S extends AbstractConstructibleBson<S>> implements Bson, ToMap {
    private static final Document EMPTY_DOC = new Document();
    /**
     * An {@linkplain Immutable immutable} {@link BsonDocument#isEmpty() empty} instance.
     */
    public static final AbstractConstructibleBson<?> EMPTY_IMMUTABLE = AbstractConstructibleBson.of(EMPTY_DOC);

    private final Bson base;
    private final Document appended;

    /**
     * This constructor is equivalent to {@link #AbstractConstructibleBson(Bson, Document)} with
     * {@link #EMPTY_IMMUTABLE} being the second argument.
     */
    protected AbstractConstructibleBson(final Bson base) {
        this(base, EMPTY_DOC);
    }

    /**
     * If both {@code base} and {@code appended} are {@link BsonDocument#isEmpty() empty},
     * then the created instance is {@linkplain Immutable immutable} provided that these constructor arguments are not mutated.
     * Otherwise, the created instance while being unmodifiable,
     * may be mutated by mutating the result of {@link #toBsonDocument(Class, CodecRegistry)}.
     */
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
                : appended.isEmpty() ? baseDoc : newMerged(baseDoc, appended.toBsonDocument(documentClass, codecRegistry));
    }

    /**
     * {@linkplain Document#append(String, Object) Appends} the specified mapping via {@link #newMutated(Consumer)}.
     *
     * @return A new instance.
     */
    protected final S newAppended(final String name, final Object value) {
        return newMutated(doc -> doc.append(name, value));
    }

    /**
     * Creates a {@link Document#Document(java.util.Map) shallow copy} of {@code this} and mutates it via the specified {@code mutator}.
     *
     * @return A new instance.
     */
    protected final S newMutated(final Consumer<Document> mutator) {
        Document newAppended = new Document(appended);
        mutator.accept(newAppended);
        return newSelf(base, newAppended);
    }

    @Override
    public Optional<Map<String, ?>> tryToMap() {
        return ToMap.tryToMap(base)
                .map(baseMap -> {
                    Map<String, Object> result = new LinkedHashMap<>(baseMap);
                    result.putAll(appended);
                    return result;
                });
    }

    public static AbstractConstructibleBson<?> of(final Bson doc) {
        return doc instanceof AbstractConstructibleBson
                // prevent double wrapping
                ? (AbstractConstructibleBson<?>) doc
                : new ConstructibleBson(doc);
    }

    @Override
    public final boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractConstructibleBson<?> that = (AbstractConstructibleBson<?>) o;
        return Objects.equals(base, that.base) && Objects.equals(appended, that.appended);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(base, appended);
    }

    @Override
    public String toString() {
        return tryToMap()
                .map(Document::new)
                .map(Document::toString)
                .orElseGet(() -> "ConstructibleBson{base=" + base
                        + ", appended=" + appended
                        + '}');
    }

    static BsonDocument newMerged(final BsonDocument base, final BsonDocument appended) {
        BsonDocument result = mutableDeepCopy(base);
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
