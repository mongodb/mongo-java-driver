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

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import static com.mongodb.internal.client.model.AbstractConstructibleBson.EMPTY_IMMUTABLE;
import static com.mongodb.internal.client.model.AbstractConstructibleBson.newMerged;
import static java.lang.String.format;

/**
 * A {@link Bson} that contains exactly one name/value pair
 * and allows constructing new instances via {@link #newWithAppendedValue(String, Object)} instead of mutating {@code this}.
 * The value must itself be a {@code Bson}.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 *
 * @param <S> A type introduced by the concrete class that extends this abstract class.
 * @see AbstractConstructibleBson
 */
public abstract class AbstractConstructibleBsonElement<S extends AbstractConstructibleBsonElement<S>> implements Bson, ToMap {
    private final Bson baseElement;
    private final AbstractConstructibleBson<?> appendedElementValue;

    protected AbstractConstructibleBsonElement(final String name) {
        this(name, EMPTY_IMMUTABLE);
    }

    protected AbstractConstructibleBsonElement(final String name, final Bson value) {
        this(new Document(name, value));
    }

    protected AbstractConstructibleBsonElement(final Bson baseElement) {
        this(baseElement, EMPTY_IMMUTABLE);
    }

    protected AbstractConstructibleBsonElement(final Bson baseElement, final Bson appendedElementValue) {
        this.baseElement = baseElement;
        this.appendedElementValue = AbstractConstructibleBson.of(appendedElementValue);
    }

    protected abstract S newSelf(Bson baseElement, Bson appendedElementValue);

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
        return newSelf(baseElement, appendedElementValue.newMutated(mutator));
    }

    @Override
    public final <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        BsonDocument baseElementDoc = baseElement.toBsonDocument(documentClass, codecRegistry);
        if (baseElementDoc.size() != 1) {
            throw new IllegalStateException(format("baseElement must contain exactly one element, but contains %s", baseElementDoc.size()));
        }
        Map.Entry<String, BsonValue> baseElementEntry = baseElementDoc.entrySet().iterator().next();
        String baseElementName = baseElementEntry.getKey();
        BsonValue baseElementValue = baseElementEntry.getValue();
        if (!baseElementValue.isDocument()) {
            throw new IllegalStateException(format("baseElement value must be a document, but it is %s", baseElementValue.getBsonType()));
        }
        BsonDocument baseElementValueDoc = baseElementValue.asDocument();
        BsonDocument appendedElementValueDoc = appendedElementValue.toBsonDocument(documentClass, codecRegistry);
        return appendedElementValueDoc.isEmpty()
                ? baseElementDoc
                : new BsonDocument(baseElementName, newMerged(baseElementValueDoc, appendedElementValueDoc));
    }

    @Override
    public Optional<Map<String, ?>> tryToMap() {
        Map<String, ?> baseElementMap = ToMap.tryToMap(baseElement).orElse(null);
        Map<String, ?> appendedElementValueMap = baseElementMap == null ? null : appendedElementValue.tryToMap().orElse(null);
        if (baseElementMap != null && appendedElementValueMap != null) {
            if (baseElementMap.size() != 1) {
                throw new IllegalStateException(format("baseElement must contain exactly one element, but contains %s", baseElementMap.size()));
            }
            Map.Entry<String, ?> baseEntry = baseElementMap.entrySet().iterator().next();
            String elementName = baseEntry.getKey();
            return ToMap.tryToMap(baseEntry.getValue())
                    .map(elementValueMap -> {
                        Map<String, Object> result = new LinkedHashMap<>(elementValueMap);
                        result.putAll(appendedElementValueMap);
                        return result;
                    })
                    .map(mergedElementValueMap -> {
                        Map<String, Object> result = new LinkedHashMap<>();
                        result.put(elementName, new Document(mergedElementValueMap));
                        return result;
                    });
        } else {
            return Optional.empty();
        }
    }

    public static AbstractConstructibleBsonElement<?> of(final Bson baseElement) {
        return baseElement instanceof AbstractConstructibleBsonElement
                // prevent double wrapping
                ? (AbstractConstructibleBsonElement<?>) baseElement
                : new ConstructibleBsonElement(baseElement, EMPTY_IMMUTABLE);
    }

    @Override
    public final boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractConstructibleBsonElement<?> that = (AbstractConstructibleBsonElement<?>) o;
        return baseElement.equals(that.baseElement) && appendedElementValue.equals(that.appendedElementValue);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(baseElement, appendedElementValue);
    }

    @Override
    public String toString() {
        return tryToMap()
                .map(Document::new)
                .map(Document::toString)
                .orElseGet(() -> "ConstructibleBsonElement{baseElement=" + baseElement
                        + ", appendedElementValue=" + appendedElementValue
                        + '}');
    }

    private static final class ConstructibleBsonElement extends AbstractConstructibleBsonElement<ConstructibleBsonElement> {
        private ConstructibleBsonElement(final Bson baseElement, final Bson appendedElementValue) {
            super(baseElement, appendedElementValue);
        }

        @Override
        protected ConstructibleBsonElement newSelf(final Bson baseElement, final Bson appendedElementValue) {
            return new ConstructibleBsonElement(baseElement, appendedElementValue);
        }
    }
}
