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

package org.mongodb.serialization;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.BsonType;
import org.mongodb.MongoException;

import java.util.HashMap;
import java.util.Map;

// TODO: this is crap, but it's a start

/**
 * Holder for all the serializer mappings.
 */
public class Serializers implements Serializer{
    private Map<Class, Serializer> classSerializerMap = new HashMap<Class, Serializer>();
    private Map<BsonType, Class> bsonTypeClassMap = new HashMap<BsonType, Class>();

    public Serializer lookup(Class clazz) {
        return classSerializerMap.get(clazz);
    }

    /**
     *
     *
     * @param clazz the class
     * @param bsonType the BSON type that this serializer handles
     * @param serializer the serializer  @return the previously registered serializer for this class
     */
    public Serializer register(Class clazz, BsonType bsonType, Serializer serializer) {
        bsonTypeClassMap.put(bsonType, clazz);
        return classSerializerMap.put(clazz, serializer);

    }

    @Override
    public void serialize(final BSONWriter writer, final Class clazz, final Object value,
                          final BsonSerializationOptions options) {
        Serializer serializer = classSerializerMap.get(clazz);
        if (serializer == null) {
            throw new MongoException("No serializer for class " + clazz);
        }
        serializer.serialize(writer, clazz, value, options);
    }

    @Override
    public Object deserialize(final BSONReader reader, final Class clazz, final BsonSerializationOptions options) {
        Serializer serializer = classSerializerMap.get(clazz);
        // TODO: handle null case
        if (serializer == null) {
            throw new MongoException("No serializer for class " + clazz);
        }
        return serializer.deserialize(reader, clazz, options);
    }

    public Class findClassByBsonType(BsonType bsonType) {
        return bsonTypeClassMap.get(bsonType);
    }
}
