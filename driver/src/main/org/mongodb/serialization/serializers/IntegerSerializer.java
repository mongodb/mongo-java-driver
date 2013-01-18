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
import org.mongodb.serialization.Serializer;

public class IntegerSerializer implements Serializer<Integer> {
    @Override
    public void serialize(final BSONWriter bsonWriter, final Integer value) {
        bsonWriter.writeInt32(value);
    }

    @Override
    public Integer deserialize(final BSONReader reader) {
        return reader.readInt32();
    }

    @Override
    public Class<Integer> getSerializationClass() {
        return Integer.class;
    }
}
