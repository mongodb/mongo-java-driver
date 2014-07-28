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

package com.mongodb.async.client;


import com.mongodb.annotations.Immutable;
import com.mongodb.async.MongoFuture;
import org.bson.codecs.Codec;
import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteResult;

import java.util.List;

/**
 * Asynchronous operations on a MongoDB collection.
 *
 * @param <T> the document type
 * @since 3.0
 */
@Immutable
public interface MongoCollection<T> {
    /**
     * Gets the name of this collection.  This is the simple name of the collection and is not prefixed with the database name.
     *
     * @return the collection name
     */
    String getName();

    /**
     * Gets the namespace of this collection.
     *
     * @return the namespace
     */
    MongoNamespace getNamespace();

    /**
     * Gets the options applied to operations on this collection.
     *
     * @return the options
     */
    MongoCollectionOptions getOptions();

    /**
     * Gets the codec used to encode and decode documents into and out of the collection.
     *
     * @return the codec
     */
    Codec<T> getCodec();

    /**
     * Create a view on the collection with the given filter. This method does not do any I/O.
     *
     * @param filter the filter
     * @return a view on this collection with the given filter
     */
    MongoView<T> find(Document filter);

    /**
     * Insert a document into the collection.
     *
     * @param document the document to insert
     * @return the result of the insert
     */
    MongoFuture<WriteResult> insert(T document);

    /**
     * Insert the documents into the collection.
     *
     * @param documents the documents to insert
     * @return the result of the insert
     */
    MongoFuture<WriteResult> insert(List<T> documents);

    /**
     * Saves a document into the collection.  If the document has no id, it is inserted.  Otherwise,
     * it is upserted using the document's id as the query filter.
     *
     * @param document the document to save
     * @return the result of the save
     */
    MongoFuture<WriteResult> save(T document);

    /**
     * @return the CollectionAdministration that provides admin methods that can be performed
     */
    CollectionAdministration tools();
}
