/*
 * Copyright 2008-present MongoDB, Inc.
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

package org.bson.codecs;

import org.bson.BsonInvalidOperationException;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;
import org.junit.Test;

import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlexibleStringCodecTest {

    @Test
    public void testDecodeOnObjectId() {
        FlexibleStringCodec codec = new FlexibleStringCodec();
        codec.setBsonRep(BsonType.OBJECT_ID);

        BsonReader reader = new JsonReader("{'_id':  ObjectId('5f5a6cc03237b5e06d6b887b'), 'name': 'Brian'}");
        reader.readStartDocument();
        reader.readName();
        String stringId = codec.decode(reader, DecoderContext.builder().build());

        assertEquals(stringId, "5f5a6cc03237b5e06d6b887b");
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void testDecodeOnObjectIdWithoutBsonRep() {
        FlexibleStringCodec codec = new FlexibleStringCodec();

        BsonReader reader = new JsonReader("{'_id':  ObjectId('5f5a6cc03237b5e06d6b887b'), 'name': 'Brian'}");
        reader.readStartDocument();
        reader.readName();
        codec.decode(reader, DecoderContext.builder().build());
    }

    @Test
    public void testEncodeWithBsonRep() {
        FlexibleStringCodec codec = new FlexibleStringCodec();
        codec.setBsonRep(BsonType.OBJECT_ID);

        StringWriter writer = new StringWriter();
        BsonWriter jsonWriter = new JsonWriter(writer);
        jsonWriter.writeStartDocument();
        jsonWriter.writeName("_id");

        codec.encode(jsonWriter, "5f5a6cc03237b5e06d6b887b", EncoderContext.builder().build());

        jsonWriter.writeEndDocument();

        assertEquals(writer.toString(), "{\"_id\": {\"$oid\": \"5f5a6cc03237b5e06d6b887b\"}}");
    }

    @Test
    public void testEncodeWithoutBsonRep() {
        FlexibleStringCodec codec = new FlexibleStringCodec();

        StringWriter writer = new StringWriter();
        BsonWriter jsonWriter = new JsonWriter(writer);
        jsonWriter.writeStartDocument();
        jsonWriter.writeName("_id");

        codec.encode(jsonWriter, "5f5a6cc03237b5e06d6b887b", EncoderContext.builder().build());

        jsonWriter.writeEndDocument();

        assertEquals(writer.toString(), "{\"_id\": \"5f5a6cc03237b5e06d6b887b\"}");
    }
}
