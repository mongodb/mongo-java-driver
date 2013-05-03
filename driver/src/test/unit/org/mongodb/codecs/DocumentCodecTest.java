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
import org.bson.BSONBinarySubType;
import org.bson.BSONBinaryWriter;
import org.bson.BSONReaderSettings;
import org.bson.BSONWriter;
import org.bson.io.BasicInputBuffer;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.InputBuffer;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWithScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Document;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class DocumentCodecTest {
    private BasicOutputBuffer buffer;
    private DocumentCodec documentCodec;
    private BSONWriter writer;

    @Before
    public void setUp() throws Exception {
        buffer = new BasicOutputBuffer();
        writer = new BSONBinaryWriter(buffer);
        documentCodec = new DocumentCodec(PrimitiveCodecs.createDefault());
    }

    @Test
    public void testPrimitiveBSONTypeCodecs() throws IOException {
        final Document doc = new Document();
        doc.put("oid", new ObjectId());
        doc.put("integer", 1);
        doc.put("long", 2L);
        doc.put("string", "hello");
        doc.put("double", 3.2);
        doc.put("binary", new Binary(BSONBinarySubType.UserDefined, new byte[]{0, 1, 2, 3}));
        doc.put("date", new Date(1000));
        doc.put("boolean", true);
        doc.put("code", new Code("var i = 0"));
        doc.put("minkey", new MinKey());
        doc.put("maxkey", new MaxKey());
        //        doc.put("pattern", Pattern.compile("^hello"));  // TODO: Pattern doesn't override equals method!
        doc.put("null", null);

        documentCodec.encode(writer, doc);

        final InputBuffer inputBuffer = createInputBuffer();
        final Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(), inputBuffer));
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void testIterableEncoding() throws IOException {
        final Document doc = new Document();
        doc.put("array", asList(1, 2, 3, 4, 5));

        documentCodec.encode(writer, doc);

        final InputBuffer inputBuffer = createInputBuffer();
        final Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(),
                                                                                   inputBuffer));
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void testCodeWithScopeEncoding() throws IOException {
        final Document doc = new Document();
        doc.put("theCode", new CodeWithScope("javaScript code", new Document("fieldNameOfScope", "valueOfScope")));

        documentCodec.encode(writer, doc);

        final Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(),
                                                                                   createInputBuffer()));
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void testIterableContainingOtherIterableEncoding() throws IOException {
        final Document doc = new Document();
        @SuppressWarnings("unchecked")
        final List<List<Integer>> listOfLists = asList(asList(1), asList(2));
        doc.put("array", listOfLists);

        documentCodec.encode(writer, doc);

        final InputBuffer inputBuffer = createInputBuffer();
        final Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(),
                                                                                   inputBuffer));
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void testIterableContainingDocumentsEncoding() throws IOException {
        final Document doc = new Document();
        final List<Document> listOfDocuments = asList(new Document("intVal", 1), new Document("anotherInt", 2));
        doc.put("array", listOfDocuments);

        documentCodec.encode(writer, doc);

        final InputBuffer inputBuffer = createInputBuffer();
        final Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(),
                                                                                   inputBuffer));
        assertEquals(doc, decodedDocument);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldEncodeArrayContainingDocumentsAndDecodeAsList() throws IOException {
        final Document doc = new Document();
        final Document[] arrayOfDocuments = {new Document("intVal", 1), new Document("anotherInt", 2)};
        doc.put("array", arrayOfDocuments);

        documentCodec.encode(writer, doc);

        final InputBuffer inputBuffer = createInputBuffer();
        final List<Document> expectedListOfDocuments = asList(new Document("intVal", 1),
                                                              new Document("anotherInt", 2));
        final Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(),
                                                                                   inputBuffer));
        assertThat((List<Document>) decodedDocument.get("array"), is(expectedListOfDocuments));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldEncodeArrayOfIntsAndDecodeAsArrayListOfIntegers() throws IOException {
        final Document doc = new Document();
        doc.put("array", new int[]{1, 2, 3, 4, 5});

        documentCodec.encode(writer, doc);

        final InputBuffer inputBuffer = createInputBuffer();
        final Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(),
                                                                                   inputBuffer));

        final List<Integer> value = asList(1, 2, 3, 4, 5);
        assertThat((List<Integer>) decodedDocument.get("array"), is(value));
    }

    @Test
    public void testMapContainingDocumentsEncoding() throws IOException {
        final Document doc = new Document();
        final Map<String, Document> mapOfDocuments = new HashMap<String, Document>();
        mapOfDocuments.put("firstDoc", new Document("intVal", 1));
        mapOfDocuments.put("secondDoc", new Document("anotherInt", 2));
        doc.put("theMap", mapOfDocuments);

        documentCodec.encode(writer, doc);

        final InputBuffer inputBuffer = createInputBuffer();
        final Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(),
                                                                                   inputBuffer));
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void testMapContainingStringsEncoding() throws IOException {
        final Document doc = new Document();
        final Map<String, String> mapOfStrings = new HashMap<String, String>();
        mapOfStrings.put("firstString", "the first string");
        mapOfStrings.put("secondString", "the second string");
        doc.put("theMap", mapOfStrings);

        documentCodec.encode(writer, doc);

        final InputBuffer inputBuffer = createInputBuffer();
        final Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(),
                                                                                   inputBuffer));
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void testNestedDocumentEncoding() throws IOException {
        final Document doc = new Document();
        doc.put("nested", new Document("x", 1));

        documentCodec.encode(writer, doc);

        final InputBuffer inputBuffer = createInputBuffer();
        final Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(),
                                                                                   inputBuffer));
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void shouldNotThrowAnExceptionForValidQueryDocumentFieldNames() throws IOException {
        final Document document = new Document("x.y", 1);

        documentCodec.encode(writer, document);

        final InputBuffer inputBuffer = createInputBuffer();
        final Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(),
                                                                                   inputBuffer));
        assertEquals(document, decodedDocument);
    }

    @Test
    public void shouldNotThrowAnExceptionForValidNestedQueryDocumentFieldNames() throws IOException {
        final Document document = new Document("x", new Document("a.b", 1));

        documentCodec.encode(writer, document);

        final InputBuffer inputBuffer = createInputBuffer();
        final Document decodedDocument = documentCodec.decode(new BSONBinaryReader(new BSONReaderSettings(),
                                                                                   inputBuffer));
        assertEquals(document, decodedDocument);
    }

    // TODO: factor into common base class;
    private InputBuffer createInputBuffer() throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        buffer.pipe(baos);
        return new BasicInputBuffer(ByteBuffer.wrap(baos.toByteArray()));
    }
}
