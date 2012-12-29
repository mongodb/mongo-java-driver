/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb.serialization.serializers;

import org.bson.BSONWriter;
import org.bson.types.Document;
import org.mongodb.serialization.BsonSerializationOptions;
import org.mongodb.serialization.CollectibleSerializer;
import org.mongodb.serialization.IdGenerator;
import org.mongodb.serialization.PrimitiveSerializers;

/**
 * Serializer for documents that go in collections, and therefore have an _id.  Ensures that the _id field is written
 * first.
 */
public class CollectibleDocumentSerializer extends DocumentSerializer implements CollectibleSerializer<Document> {
    public static final String ID_FIELD_NAME = "_id";
    private final IdGenerator idGenerator;

    public CollectibleDocumentSerializer(final PrimitiveSerializers primitiveSerializers,
                                         final IdGenerator idGenerator) {
        super(primitiveSerializers);
        if (idGenerator == null) {
            throw new IllegalArgumentException("idGenerator is null");
        }
        this.idGenerator = idGenerator;
    }

    @Override
    protected void beforeFields(final BSONWriter bsonWriter, final Document document,
                                final BsonSerializationOptions options) {
        if (document.get(ID_FIELD_NAME) == null) {
            document.put(ID_FIELD_NAME, idGenerator.generate());
        }
        bsonWriter.writeName(ID_FIELD_NAME);
        writeValue(bsonWriter, document.get(ID_FIELD_NAME), options);
    }

    @Override
    protected boolean skipField(String key) {
        return key.equals(ID_FIELD_NAME);
    }

    @Override
    protected void validateFieldName(final String key) {
        if (key == null) {
            throw new IllegalArgumentException("key can not be null");
        }

        if (key.contains(".")) {
            throw new IllegalArgumentException(
                    "fields stored in the db can't have . in them. (Bad Key: '" + key + "')");
        }
        if (key.startsWith("$")) {
            throw new IllegalArgumentException("fields stored in the db can't start with '$' (Bad Key: '" + key + "')");
        }
    }

    @Override
    public Object getId(final Document document) {
        return document.get(ID_FIELD_NAME);
    }
}
