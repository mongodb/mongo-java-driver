/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.bson.types;

import org.bson.BsonDocumentWriter;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonWriter;
import org.bson.codecs.Encoder;

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
public class BsonDocumentWrapper<T> extends BsonDocument {

    private static final long serialVersionUID = 4164303588753136248L;

    private final transient T document;
    private final transient Encoder<T> encoder;
    private BsonDocument unwrapped;

    /**
     * Construct a new instance with the given document and encoder for the document.
     *
     * @param document the wrapped document
     * @param encoder  the encoder for the wrapped document
     */
    public BsonDocumentWrapper(final T document, final Encoder<T> encoder) {
        if (document == null) {
            throw new IllegalArgumentException("Document can not be null");
        }
        this.document = document;
        this.encoder = encoder;
    }

    public T getDocument() {
        return document;
    }

    public Encoder<T> getEncoder() {
        return encoder;
    }

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

    private BsonDocument getUnwrapped() {
        if (encoder == null) {
            throw new BsonInvalidOperationException("Can not unwrap a BsonDocumentWrapper with no Encoder");
        }
        if (unwrapped == null) {
            BsonDocument unwrapped = new BsonDocument();
            BsonWriter writer = new BsonDocumentWriter(unwrapped);
            encoder.encode(writer, document);
            this.unwrapped = unwrapped;
        }
        return unwrapped;
    }
}
