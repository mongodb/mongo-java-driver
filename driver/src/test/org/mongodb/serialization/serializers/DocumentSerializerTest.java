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
 */

package org.mongodb.serialization.serializers;

import org.bson.BSONBinaryReader;
import org.bson.BSONBinaryWriter;
import org.bson.BSONWriter;
import org.bson.BinaryWriterSettings;
import org.bson.BsonReaderSettings;
import org.bson.BsonWriterSettings;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferInput;
import org.bson.io.InputBuffer;
import org.bson.types.Binary;
import org.bson.types.Document;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.MongoClientTestBase;
import org.mongodb.serialization.PrimitiveSerializers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;

import static org.junit.Assert.assertEquals;

// straight up unit test
public class DocumentSerializerTest extends MongoClientTestBase {
    private BasicOutputBuffer buffer;
    private DocumentSerializer serializer;
    private BSONWriter writer;

    @Before
    public void setUp() {
        buffer = new BasicOutputBuffer();
        writer = new BSONBinaryWriter(new BsonWriterSettings(100), new BinaryWriterSettings(1024 * 1024), buffer);
        serializer = new DocumentSerializer(PrimitiveSerializers.createDefault());
    }

    @Test
    public void testPrimitiveBsonTypeSerialization() throws IOException {
        final Document doc = new Document();
        doc.put("oid", new ObjectId());
        doc.put("integer", 1);
        doc.put("long", 2L);
        doc.put("string", "hello");
        doc.put("double", 3.2);
        doc.put("binary", new Binary(new byte[]{0, 1, 2, 3}));
        doc.put("date", new Date(1000));
        doc.put("boolean", true);
//        doc.put("pattern", Pattern.compile("^hello"));  // TODO: Pattern doesn't override equals method!
        doc.put("null", null);

        serializer.serialize(writer, doc, null);

        final InputBuffer inputBuffer = createInputBuffer();
        final Document deserializedDoc = serializer.deserialize(new BSONBinaryReader(new BsonReaderSettings(),
                                                                                     inputBuffer), null);
        assertEquals(doc, deserializedDoc);
    }


    @Test
    public void testArraySerialization() throws IOException {
        final Document doc = new Document();
        doc.put("array", Arrays.asList(1, 2, 3, 4, 5));

        serializer.serialize(writer, doc, null);

        final InputBuffer inputBuffer = createInputBuffer();
        final Document deserializedDoc = serializer.deserialize(new BSONBinaryReader(new BsonReaderSettings(),
                                                                                     inputBuffer), null);
        assertEquals(doc, deserializedDoc);
    }

    @Test
    public void testNestedDocumentSerialization() throws IOException {
        final Document doc = new Document();
        doc.put("nested", new Document("x", 1));

        serializer.serialize(writer, doc, null);

        final InputBuffer inputBuffer = createInputBuffer();
        final Document deserializedDoc = serializer.deserialize(new BSONBinaryReader(new BsonReaderSettings(),
                                                                                     inputBuffer), null);
        assertEquals(doc, deserializedDoc);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDotsInKeys() {
        getCollection().save(new Document("x.y", 1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDotsInKeysInNestedDocuments() {
        getCollection().save(new Document("x", new Document("a.b", 1)));
    }

    // TODO: factor into common base class;
    private InputBuffer createInputBuffer() throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        buffer.pipe(baos);
        return new ByteBufferInput(ByteBuffer.wrap(baos.toByteArray()));
    }

}
