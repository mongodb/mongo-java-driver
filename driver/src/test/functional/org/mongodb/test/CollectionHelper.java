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

package org.mongodb.test;

import org.mongodb.Codec;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoCursor;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.operation.CountOperation;
import org.mongodb.operation.Find;
import org.mongodb.operation.InsertOperation;
import org.mongodb.operation.InsertRequest;
import org.mongodb.operation.QueryOperation;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.mongodb.Fixture.getBinding;

public final class CollectionHelper<T> {

    private Codec<T> codec;
    private MongoNamespace namespace;
    private Encoder<Document> queryEncoder;

    public CollectionHelper(final Codec<T> codec, final MongoNamespace namespace) {
        this.codec = codec;
        this.namespace = namespace;
        this.queryEncoder = new DocumentCodec(PrimitiveCodecs.createDefault());
    }

    @SuppressWarnings("unchecked")
    public void insertDocuments(final T... documents) {
        for (T document : documents) {
            new InsertOperation<T>(namespace, true, WriteConcern.ACKNOWLEDGED,
                    asList(new InsertRequest<T>(document)), codec).execute(getBinding());
        }
    }

    public List<T> find() {
        MongoCursor<T> cursor = new QueryOperation<T>(namespace, new Find(), queryEncoder, codec).execute(getBinding());
        List<T> results = new ArrayList<T>();
        while (cursor.hasNext()) {
            results.add(cursor.next());
        }
        return results;
    }

    public List<T> find(Document filter) {
        MongoCursor<T> cursor = new QueryOperation<T>(namespace, new Find(filter), queryEncoder, codec).execute(getBinding());
        List<T> results = new ArrayList<T>();
        while (cursor.hasNext()) {
            results.add(cursor.next());
        }
        return results;
    }

    public long count() {
        return new CountOperation(namespace, new Find(), new DocumentCodec()).execute(getBinding());
    }

    public long count(Document filter) {
        return new CountOperation(namespace, new Find(filter), new DocumentCodec()).execute(getBinding());
    }
}
