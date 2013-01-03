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

package com.mongodb.serializers;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import org.bson.BSONWriter;
import org.mongodb.serialization.BsonSerializationOptions;
import org.mongodb.serialization.CollectibleSerializer;
import org.mongodb.serialization.IdGenerator;
import org.mongodb.serialization.PrimitiveSerializers;

/**
 * Serializer for documents that go in collections, and therefore have an _id.  Ensures that the _id field is written
 * first.
 */
public class CollectibleDBObjectSerializer extends DBObjectSerializer implements CollectibleSerializer<DBObject> {
    public static final String ID_FIELD_NAME = "_id";
    private final IdGenerator idGenerator;

    public CollectibleDBObjectSerializer(final DB db, final PrimitiveSerializers primitiveSerializers,
                                         final IdGenerator idGenerator) {
        this(db, primitiveSerializers, idGenerator, BasicDBObject.class);
    }

    public CollectibleDBObjectSerializer(final DB db, final PrimitiveSerializers primitiveSerializers,
                                         final IdGenerator idGenerator,
                                         final Class<? extends DBObject> clazz) {
        super(db, primitiveSerializers, clazz);
        this.idGenerator = idGenerator;
    }

    @Override
    protected void beforeFields(final BSONWriter bsonWriter, final DBObject document,
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
    protected void validateField(final String key) {
        if (key.contains(".")) {
            throw new IllegalArgumentException(
                    "fields stored in the db can't have . in them. (Bad Key: '" + key + "')");
        }
        if (key.startsWith("$")) {
            throw new IllegalArgumentException("fields stored in the db can't start with '$' (Bad Key: '" + key + "')");
        }
    }

    @Override
    public Object getId(final DBObject document) {
        return document.get(ID_FIELD_NAME);
    }
}
