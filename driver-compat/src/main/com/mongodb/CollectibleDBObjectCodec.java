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

package com.mongodb;

import org.bson.BSONType;
import org.bson.BSONWriter;
import org.bson.codecs.configuration.CodecRegistry;
import org.mongodb.CollectibleCodec;
import org.mongodb.IdGenerator;
import org.mongodb.codecs.validators.FieldNameValidator;

import java.util.Map;

/**
 * Codec for documents that go in collections, and therefore have an _id.  Ensures that the _id field is written first.
 */
class CollectibleDBObjectCodec extends DBObjectCodec implements CollectibleCodec<DBObject> {
    private static final String ID_FIELD_NAME = "_id";
    private final IdGenerator idGenerator;

    public CollectibleDBObjectCodec(final DB database, final IdGenerator idGenerator, final DBObjectFactory objectFactory,
                                    final CodecRegistry codecRegistry, final Map<BSONType, Class<?>> bsonTypeClassMap) {
        super(database, new FieldNameValidator(), objectFactory, codecRegistry, bsonTypeClassMap);
        this.idGenerator = idGenerator;
    }

    @Override
    protected void beforeFields(final BSONWriter bsonWriter, final DBObject document) {
        if (document.get(ID_FIELD_NAME) == null) {
            document.put(ID_FIELD_NAME, idGenerator.generate());
        }
        bsonWriter.writeName(ID_FIELD_NAME);
        writeValue(bsonWriter, document.get(ID_FIELD_NAME));
    }

    @Override
    protected boolean skipField(final String key) {
        return key.equals(ID_FIELD_NAME);
    }

    @Override
    public Object getId(final DBObject document) {
        return document.get(ID_FIELD_NAME);
    }
}
