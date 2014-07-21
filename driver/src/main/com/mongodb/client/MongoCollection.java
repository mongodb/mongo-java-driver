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

package com.mongodb.client;

import com.mongodb.WriteConcern;
import com.mongodb.annotations.ThreadSafe;
import org.bson.codecs.Codec;
import org.mongodb.ConvertibleToDocument;
import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteResult;

import java.util.List;

/**
 * Additions to this interface will not be considered to break binary compatibility.
 *
 * @param <T> The type that this collection will encode documents from and decode documents to
 */
@ThreadSafe
public interface MongoCollection<T> {
    /**
     * Gets the database in which this collection resides.
     *
     * @return the database
     */
    MongoDatabase getDatabase();

    /**
     * Gets the name of this collection.  This is the simple name of the collection and is not prefixed with the database name.
     *
     * @return the collection name
     */
    String getName();

    MongoNamespace getNamespace();

    MongoCollectionOptions getOptions();

    Codec<T> getCodec();

    CollectionAdministration tools();

    MongoView<T> find();

    MongoView<T> find(Document filter);

    MongoView<T> find(ConvertibleToDocument filter);

    MongoView<T> withWriteConcern(WriteConcern writeConcern);

    WriteResult insert(T document);

    WriteResult insert(List<T> document);

    WriteResult save(T document);

    MongoPipeline<T> pipe();
}
