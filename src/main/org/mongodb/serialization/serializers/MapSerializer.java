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

import com.mongodb.MongoException;
import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.BsonType;
import org.mongodb.serialization.BsonSerializationOptions;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.Serializers;

import java.util.HashMap;
import java.util.Map;

public class MapSerializer implements Serializer {

    private final Serializers serializers;

    public MapSerializer(Serializers serializers) {
        this.serializers = serializers;
    }

    @Override
    public void serialize(final BSONWriter bsonWriter, final Class clazz, final Object value,
                          final BsonSerializationOptions options) {
        Map<String, Object> map = (Map<String, Object>) value;
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
        // TODO: allow control over Map implementation class
        Map<String, Object> map = new HashMap<String, Object>();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT)
        {
            String key = reader.readName();
            BsonType bsonType = reader.getNextBsonType();
            Class valueClass = serializers.findClassByBsonType(bsonType);
            if (valueClass == null) {
                throw new MongoException("Unable to find value class for BSON type " + bsonType);
            }
            Serializer valueSerializer = serializers.lookup(valueClass);
            if (valueSerializer == null) {
                throw new MongoException("Unable to find deserializer for class " + valueClass.getName());
            }
            Object value = valueSerializer.deserialize(reader, Object.class, options);
            map.put(key, value);
        }
        reader.readEndDocument();

        return map;
    }
}
