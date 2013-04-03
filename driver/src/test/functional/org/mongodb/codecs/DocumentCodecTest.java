/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.codecs;

import org.bson.BSONBinaryReader;
import org.bson.BSONBinaryWriter;
import org.bson.BSONBinaryWriterSettings;
import org.bson.BSONReaderSettings;
import org.bson.BSONWriter;
import org.bson.BSONWriterSettings;
import org.bson.io.BasicInputBuffer;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.InputBuffer;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.mongodb.Document;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.PrimitiveCodecs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;

import static org.junit.Assert.assertEquals;

// straight up unit test
public class DocumentCodecTest extends DatabaseTestCase {
    private BasicOutputBuffer buffer;
    private DocumentCodec codec;
    private BSONWriter writer;

    @Before
    public void setUp() {
        super.setUp();
        buffer = new BasicOutputBuffer();
        writer = new BSONBinaryWriter(new BSONWriterSettings(100), new BSONBinaryWriterSettings(1024 * 1024), buffer);
        codec = new DocumentCodec(PrimitiveCodecs.createDefault());
    }

    @Test
    public void testPrimitiveBSONTypeCodecs() throws IOException {
        final Document doc = new Document();
        doc.put("oid", new ObjectId());
        doc.put("integer", 1);
        doc.put("long", 2L);
        doc.put("string", "hello");
        doc.put("double", 3.2);
        doc.put("binary", new Binary(new byte[]{0, 1, 2, 3}));
        doc.put("date", new Date(1000));
        doc.put("boolean", true);
        doc.put("code", new Code("var i = 0"));
        doc.put("minkey", new MinKey());
        doc.put("maxkey", new MaxKey());
//        doc.put("pattern", Pattern.compile("^hello"));  // TODO: Pattern doesn't override equals method!
        doc.put("null", null);

        codec.encode(writer, doc);

        final InputBuffer inputBuffer = createInputBuffer();
        final Document decodedDocument = codec.decode(new BSONBinaryReader(new BSONReaderSettings(),
                inputBuffer));
        assertEquals(doc, decodedDocument);
    }


    @Test
    public void testArrayEncoding() throws IOException {
        final Document doc = new Document();
        doc.put("array", Arrays.asList(1, 2, 3, 4, 5));

        codec.encode(writer, doc);

        final InputBuffer inputBuffer = createInputBuffer();
        final Document decodedDocument = codec.decode(new BSONBinaryReader(new BSONReaderSettings(),
                inputBuffer));
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void testNestedDocumentEncoding() throws IOException {
        final Document doc = new Document();
        doc.put("nested", new Document("x", 1));

        codec.encode(writer, doc);

        final InputBuffer inputBuffer = createInputBuffer();
        final Document decodedDocument = codec.decode(new BSONBinaryReader(new BSONReaderSettings(),
                inputBuffer));
        assertEquals(doc, decodedDocument);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDotsInKeys() {
        collection.save(new Document("x.y", 1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDotsInKeysInNestedDocuments() {
        collection.save(new Document("x", new Document("a.b", 1)));
    }

    // TODO: factor into common base class;
    private InputBuffer createInputBuffer() throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        buffer.pipe(baos);
        return new BasicInputBuffer(ByteBuffer.wrap(baos.toByteArray()));
    }
}
