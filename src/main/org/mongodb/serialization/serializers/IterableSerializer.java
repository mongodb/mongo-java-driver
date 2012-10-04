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
import org.mongodb.serialization.BsonSerializationOptions;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.Serializers;

public class IterableSerializer implements Serializer {
    private final Serializers serializers;

    public IterableSerializer(Serializers serializers) {
        this.serializers = serializers;
    }

    @Override
    public void serialize(final BSONWriter bsonWriter, final Class clazz, final Object value,
                          final BsonSerializationOptions options) {
        Iterable collection = (Iterable) value;
        bsonWriter.writeStartArray();
        for (Object cur : collection) {
            // TODO: deal with options.  C# driver sends different options
            serializers.serialize(bsonWriter, cur.getClass(), cur, options);
        }
        bsonWriter.writeEndArray();
    }

    @Override
    public Object deserialize(final BSONReader reader, final Class clazz, final BsonSerializationOptions options) {
        throw new UnsupportedOperationException();
    }
}
