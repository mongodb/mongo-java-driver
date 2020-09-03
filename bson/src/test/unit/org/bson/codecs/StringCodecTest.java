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
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;
import org.junit.Test;

import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StringCodecTest {

    private DecoderContext decoderContext = DecoderContext.builder().build();
    private EncoderContext encoderContext = EncoderContext.builder().build();
    private Codec<String> parent =  new StringCodec();
    @SuppressWarnings("unchecked")
    private Codec<String> child = ((RepresentationConfigurable<String>) parent).withRepresentation(BsonType.OBJECT_ID);

    @Test
    public void testSettingRepresentation() {
        assertEquals(((RepresentationConfigurable) parent).getRepresentation(), BsonType.STRING);
        assertEquals(((RepresentationConfigurable) child).getRepresentation(), BsonType.OBJECT_ID);
    }

    @Test
    public void testStringRepresentation() {
        @SuppressWarnings("unchecked")
        Codec<String> child = ((RepresentationConfigurable) parent).withRepresentation(BsonType.STRING);
        assertEquals(((RepresentationConfigurable) child).getRepresentation(), BsonType.STRING);
    }

    @Test(expected = CodecConfigurationException.class)
    public void testInvalidRepresentation() {
        ((RepresentationConfigurable) parent).withRepresentation(BsonType.INT32);
    }


    @Test
    public void testDecodeOnObjectIdWithObjectIdRep() {
        BsonReader reader = new JsonReader("{'_id':  ObjectId('5f5a6cc03237b5e06d6b887b'), 'name': 'Brian'}");
        reader.readStartDocument();
        reader.readName();
        String stringId = child.decode(reader, decoderContext);

        assertEquals(stringId, "5f5a6cc03237b5e06d6b887b");
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void testDecodeOnObjectIdWithStringRep() {
        BsonReader reader = new JsonReader("{'_id':  ObjectId('5f5a6cc03237b5e06d6b887b'), 'name': 'Brian'}");
        reader.readStartDocument();
        reader.readName();
        parent.decode(reader, decoderContext);
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void testDecodeOnStringWithObjectIdRep() {
        BsonReader reader = new JsonReader("{'name': 'Brian'");
        reader.readStartDocument();
        reader.readName();
        child.decode(reader, decoderContext);
    }

    @Test
    public void testDecodeOnStringWithStringRep() {
        BsonReader reader = new JsonReader("{'name': 'Brian'");
        reader.readStartDocument();
        reader.readName();
        assertEquals(parent.decode(reader, decoderContext), "Brian");
    }

    @Test
    public void testEncodeWithObjectIdRep() {
        StringWriter writer = new StringWriter();
        BsonWriter jsonWriter = new JsonWriter(writer);
        jsonWriter.writeStartDocument();
        jsonWriter.writeName("_id");

        child.encode(jsonWriter, "5f5a6cc03237b5e06d6b887b", encoderContext);

        jsonWriter.writeEndDocument();

        assertEquals(writer.toString(), "{\"_id\": {\"$oid\": \"5f5a6cc03237b5e06d6b887b\"}}");
    }

    @Test
    public void testEncodeWithStringRep() {
        StringWriter writer = new StringWriter();
        BsonWriter jsonWriter = new JsonWriter(writer);
        jsonWriter.writeStartDocument();
        jsonWriter.writeName("_id");

        parent.encode(jsonWriter, "5f5a6cc03237b5e06d6b887b", EncoderContext.builder().build());

        jsonWriter.writeEndDocument();

        assertEquals(writer.toString(), "{\"_id\": \"5f5a6cc03237b5e06d6b887b\"}");
    }
}
