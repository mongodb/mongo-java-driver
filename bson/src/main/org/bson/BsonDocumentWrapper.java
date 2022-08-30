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

import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * A {@code BsonDocument} that begins its life as a document of any type and an {@code Encoder} for that document, which lets an instance of
 * any class with an Encoder be treated as a BsonDocument. If any methods are called which required access to the individual elements of the
 * document, then, on demand, the document will be unwrapped into a BsonDocument using a {@code BsonDocumentWriter} and the {@code Encoder}.
 * But if all that is done with this document is to encode it, then the {@code Encoder} will be used to do that.
 *
 * @param <T> the type of the document that is wrapped
 * @see org.bson.BsonDocumentWriter
 * @since 3.0
 */
public final class BsonDocumentWrapper<T> extends BsonDocument {
    private static final long serialVersionUID = 1L;

    private final transient T wrappedDocument;
    private final transient Encoder<T> encoder;

    /**
     * The unwrapped document, which may be null
     */
    private BsonDocument unwrapped;

    /**
     * A helper to convert a document of type Object to a BsonDocument
     *
     * <p>If not already a BsonDocument it looks up the documents' class in the codecRegistry and wraps it into a BsonDocumentWrapper</p>
     *
     * @param document      the document to convert
     * @param codecRegistry the codecRegistry that can be used in the conversion of the Object
     * @return a BsonDocument
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static BsonDocument asBsonDocument(final Object document, final CodecRegistry codecRegistry) {
        if (document == null) {
            return null;
        }
        if (document instanceof BsonDocument) {
            return (BsonDocument) document;
        } else {
            return new BsonDocumentWrapper(document, codecRegistry.get(document.getClass()));
        }
    }

    /**
     * Construct a new instance with the given document and encoder for the document.
     *
     * @param wrappedDocument the wrapped document
     * @param encoder  the encoder for the wrapped document
     */
    public BsonDocumentWrapper(final T wrappedDocument, final Encoder<T> encoder) {
        if (wrappedDocument == null) {
            throw new IllegalArgumentException("Document can not be null");
        }
        this.wrappedDocument = wrappedDocument;
        this.encoder = encoder;
    }

    /**
     * Get the wrapped document.
     *
     * @return the wrapped document
     */
    public T getWrappedDocument() {
        return wrappedDocument;
    }

    /**
     * Get the encoder to use for the wrapped document.
     *
     * @return the encoder
     */
    public Encoder<T> getEncoder() {
        return encoder;
    }

    /**
     * Determine whether the document has been unwrapped already.
     *
     * @return true if the wrapped document has been unwrapped already
     */
    public boolean isUnwrapped() {
        return unwrapped != null;
    }

    @Override
    public int size() {
        return getUnwrapped().size();
    }

    @Override
    public boolean isEmpty() {
        return getUnwrapped().isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return getUnwrapped().containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return getUnwrapped().containsValue(value);
    }

    @Override
    public BsonValue get(final Object key) {
        return getUnwrapped().get(key);
    }

    @Override
    public BsonValue put(final String key, final BsonValue value) {
        return getUnwrapped().put(key, value);
    }

    @Override
    public BsonValue remove(final Object key) {
        return getUnwrapped().remove(key);
    }

    @Override
    public void putAll(final Map<? extends String, ? extends BsonValue> m) {
        super.putAll(m);
    }

    @Override
    public void clear() {
        super.clear();
    }

    @Override
    public Set<String> keySet() {
        return getUnwrapped().keySet();
    }

    @Override
    public Collection<BsonValue> values() {
        return getUnwrapped().values();
    }

    @Override
    public Set<Entry<String, BsonValue>> entrySet() {
        return getUnwrapped().entrySet();
    }

    @Override
    public boolean equals(final Object o) {
        return getUnwrapped().equals(o);
    }

    @Override
    public int hashCode() {
        return getUnwrapped().hashCode();
    }

    @Override
    public String toString() {
        return getUnwrapped().toString();
    }

    @Override
    public BsonDocument clone() {
        return getUnwrapped().clone();
    }

    private BsonDocument getUnwrapped() {
        if (encoder == null) {
            throw new BsonInvalidOperationException("Can not unwrap a BsonDocumentWrapper with no Encoder");
        }
        if (unwrapped == null) {
            BsonDocument unwrapped = new BsonDocument();
            BsonWriter writer = new BsonDocumentWriter(unwrapped);
            encoder.encode(writer, wrappedDocument, EncoderContext.builder().build());
            this.unwrapped = unwrapped;
        }
        return unwrapped;
    }

    /**
     * Write the replacement object.
     *
     * <p>
     * See https://docs.oracle.com/javase/6/docs/platform/serialization/spec/output.html
     * </p>
     *
     * @return a proxy for the document
     */
    private Object writeReplace() {
        return getUnwrapped();
    }

    /**
     * Prevent normal deserialization.
     *
     * <p>
     * See https://docs.oracle.com/javase/6/docs/platform/serialization/spec/input.html
     * </p>
     *
     * @param stream the stream
     * @throws InvalidObjectException in all cases
     */
    private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
        throw new InvalidObjectException("Proxy required");
    }
}
