/**
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

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.BsonType;
import org.mongodb.MongoDocument;
import org.mongodb.MongoException;
import org.mongodb.serialization.BsonSerializationOptions;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.Serializers;

import java.util.Map;

public class MongoDocumentSerializer implements Serializer {

    private final Serializers serializers;

    public MongoDocumentSerializer(Serializers serializers) {
        this.serializers = serializers;
    }

    @Override
    public void serialize(final BSONWriter bsonWriter, final Class clazz, final Object value,
                          final BsonSerializationOptions options) {
        MongoDocument map = (MongoDocument) value;
        bsonWriter.writeStartDocument();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            bsonWriter.writeName(entry.getKey());
            // TODO: deal with options.  C# driver sends different options.  For one, to write _id field first
            serializers.serialize(bsonWriter, entry.getValue().getClass(), entry.getValue(), options);
        }
        bsonWriter.writeEndDocument();
    }

    @Override
    public Object deserialize(final BSONReader reader, final Class clazz, final BsonSerializationOptions options) {
        MongoDocument map = new MongoDocument();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT)
        {
            String fieldName = reader.readName();
            BsonType bsonType = reader.getNextBsonType();
            Class valueClass = getClassByBsonType(bsonType, fieldName);
            if (valueClass == null) {
                throw new MongoException("Unable to find value class for BSON type " + bsonType + " of field " + fieldName);
            }
            Serializer valueSerializer = serializers.lookup(valueClass);
            if (valueSerializer == null) {
                throw new MongoException("Unable to find deserializer for class " + valueClass.getName());
            }
            Object value = valueSerializer.deserialize(reader, Object.class, options);
            map.put(fieldName, value);
        }
        reader.readEndDocument();

        return map;
    }

    protected Class getClassByBsonType(final BsonType bsonType, String fieldName) {
        return serializers.findClassByBsonType(bsonType);
    }

    protected Serializers getSerializers() {
        return serializers;
    }
}