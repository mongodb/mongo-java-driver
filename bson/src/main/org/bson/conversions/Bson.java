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

package org.bson.conversions;

import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;

/**
 * An interface for types that are able to render themselves into a {@code BsonDocument}.
 *
 * @since 3.0
 */
public interface Bson {
    /**
     * Render the filter into a BsonDocument.
     *
     * @param documentClass the document class in scope for the collection.  This parameter may be ignored, but it may be used to alter
     *                      the structure of the returned {@code BsonDocument} based on some knowledge of the document class.
     * @param codecRegistry the codec registry.  This parameter may be ignored, but it may be used to look up {@code Codec} instances for
     *                      the document class or any other related class.
     * @param <TDocument> the type of the document class
     * @return the BsonDocument
     */
    <TDocument> BsonDocument toBsonDocument(Class<TDocument> documentClass, CodecRegistry codecRegistry);
}
