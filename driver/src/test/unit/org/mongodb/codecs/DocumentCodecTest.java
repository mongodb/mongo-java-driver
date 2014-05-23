/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
import org.bson.BSONBinarySubType;
import org.bson.BSONBinaryWriter;
import org.bson.BSONReaderSettings;
import org.bson.BSONWriter;
import org.bson.ByteBufNIO;
import org.bson.io.BasicInputBuffer;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.InputBuffer;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.CodeWithScope;
import org.mongodb.Document;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class DocumentCodecTest {
    private BasicOutputBuffer buffer;
    private DocumentCodec documentCodec;
    private BSONWriter writer;

    @Before
    public void setUp() throws Exception {
        buffer = new BasicOutputBuffer();
        writer = new BSONBinaryWriter(buffer, true);
        documentCodec = new DocumentCodec();
    }

    @After
    public void tearDown() {
        writer.close();
    }

    @Test
    public void testPrimitiveBSONTypeCodecs() throws IOException {
        Document doc = new Document();
        doc.put("oid", new ObjectId());
        doc.put("integer", 1);
        doc.put("long", 2L);
        doc.put("string", "hello");
        doc.put("double", 3.2);
        doc.put("binary", new Binary(BSONBinarySubType.USER_DEFINED, new byte[]{0, 1, 2, 3}));
        doc.put("date", new Date(1000));
        doc.put("boolean", true);
        doc.put("code", new Code("var i = 0"));
        doc.put("minkey", new MinKey());
        doc.put("maxkey", new MaxKey());
        //        doc.put("pattern", Pattern.compile("^hello"));  // TODO: Pattern doesn't override equals method!
        doc.put("null", null);

        documentCodec.encode(writer, doc);

        InputBuffer inputBuffer = createInputBuffer();
        Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(), inputBuffer, false));
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void testIterableEncoding() throws IOException {
        Document doc = new Document();
        doc.put("array", asList(1, 2, 3, 4, 5));

        documentCodec.encode(writer, doc);

        InputBuffer inputBuffer = createInputBuffer();
        Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(), inputBuffer, false));
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void testCodeWithScopeEncoding() throws IOException {
        Document doc = new Document();
        doc.put("theCode", new CodeWithScope("javaScript code", new Document("fieldNameOfScope", "valueOfScope")));

        documentCodec.encode(writer, doc);

        Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(), createInputBuffer(), false));
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void testIterableContainingOtherIterableEncoding() throws IOException {
        Document doc = new Document();
        @SuppressWarnings("unchecked")
        List<List<Integer>> listOfLists = asList(asList(1), asList(2));
        doc.put("array", listOfLists);

        documentCodec.encode(writer, doc);

        InputBuffer inputBuffer = createInputBuffer();
        Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(), inputBuffer, false));
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void testIterableContainingDocumentsEncoding() throws IOException {
        Document doc = new Document();
        List<Document> listOfDocuments = asList(new Document("intVal", 1), new Document("anotherInt", 2));
        doc.put("array", listOfDocuments);

        documentCodec.encode(writer, doc);

        InputBuffer inputBuffer = createInputBuffer();
        Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(), inputBuffer, false));
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void testNestedDocumentEncoding() throws IOException {
        Document doc = new Document();
        doc.put("nested", new Document("x", 1));

        documentCodec.encode(writer, doc);

        InputBuffer inputBuffer = createInputBuffer();
        Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(), inputBuffer, false));
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void shouldNotThrowAnExceptionForValidQueryDocumentFieldNames() throws IOException {
        Document document = new Document("x.y", 1);

        documentCodec.encode(writer, document);

        InputBuffer inputBuffer = createInputBuffer();
        Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(), inputBuffer, false));
        assertEquals(document, decodedDocument);
    }

    @Test
    public void shouldNotThrowAnExceptionForValidNestedQueryDocumentFieldNames() throws IOException {
        Document document = new Document("x", new Document("a.b", 1));

        documentCodec.encode(writer, document);

        InputBuffer inputBuffer = createInputBuffer();
        Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(), inputBuffer, false));
        assertEquals(document, decodedDocument);
    }

    // TODO: factor into common base class;
    private InputBuffer createInputBuffer() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        buffer.pipe(baos);
        return new BasicInputBuffer(new ByteBufNIO(ByteBuffer.wrap(baos.toByteArray())));
    }
}
