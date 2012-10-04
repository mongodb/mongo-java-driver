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

import org.bson.BSONBinaryWriter;
import org.bson.BinaryWriterSettings;
import org.bson.BsonType;
import org.bson.BsonWriterSettings;
import org.bson.io.BasicOutputBuffer;
import org.mongodb.serialization.Serializers;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class MapSerializerTest extends Assert {
    MapSerializer mapSerializer;
    private BSONBinaryWriter bsonWriter;
    private BasicOutputBuffer buffer;

    @BeforeTest
    public void setUp() {
        Serializers serializers = new Serializers();
        mapSerializer = new MapSerializer(serializers);
        serializers.register(Map.class, BsonType.DOCUMENT, mapSerializer);

        buffer = new BasicOutputBuffer();
        BsonWriterSettings settings = new BsonWriterSettings(100);
        BinaryWriterSettings binaryWriterSettings = new BinaryWriterSettings(1024 * 1024);
        bsonWriter = new BSONBinaryWriter(settings, binaryWriterSettings, buffer);
    }

    @Test
    public void testMapSerializer() {
        Map<String, Object> map = new HashMap<String, Object>();
        mapSerializer.serialize(bsonWriter, Map.class, map, null);
        // TODO: what can I assert?
    }
}
