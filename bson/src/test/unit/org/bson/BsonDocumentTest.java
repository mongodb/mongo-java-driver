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

package org.bson;

import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonMode;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;
import org.junit.Test;

import java.io.StringWriter;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// Don't convert to Spock, as Groovy intercepts equals/hashCode methods that we are trying to test
public class BsonDocumentTest {
    private BsonDocument emptyDocument = new BsonDocument();
    private BsonDocument emptyRawDocument = new RawBsonDocument(emptyDocument, new BsonDocumentCodec());
    private BsonDocument document = new BsonDocument()
                                    .append("a", new BsonInt32(1))
                                    .append("b", new BsonInt32(2))
                                    .append("c", new BsonDocument("x", BsonBoolean.TRUE))
                                    .append("d", new BsonArray(Arrays.<BsonValue>asList(new BsonDocument("y",
                                                                                                         BsonBoolean.FALSE),
                                                                                        new BsonInt32(1))));

    private BsonDocument rawDocument = new RawBsonDocument(document, new BsonDocumentCodec());

    @Test
    public void shouldBeEqualToItself() {
        assertTrue(emptyDocument.equals(emptyDocument));
        assertTrue(document.equals(document));
    }

    @Test
    public void shouldBeEqualToEquivalentBsonDocument() {
        assertTrue(emptyDocument.equals(emptyRawDocument));
        assertTrue(document.equals(rawDocument));
        assertTrue(emptyRawDocument.equals(emptyDocument));
        assertTrue(rawDocument.equals(document));
    }

    @Test
    public void shouldNotBeEqualToDifferentBsonDocument() {
        // expect
        assertFalse(emptyDocument.equals(document));
        assertFalse(document.equals(emptyRawDocument));
        assertFalse(document.equals(emptyRawDocument));
        assertFalse(emptyRawDocument.equals(document));
        assertFalse(rawDocument.equals(emptyDocument));
    }

    @Test
    public void shouldHaveSameHashCodeAsEquivalentBsonDocument() {
        assertEquals(emptyDocument.hashCode(), new BsonDocument().hashCode());
        assertEquals(emptyDocument.hashCode(), emptyRawDocument.hashCode());
        assertEquals(document.hashCode(), rawDocument.hashCode());
    }

    @Test
    public void toJsonShouldReturnEquivalent() {
        assertEquals(new BsonDocumentCodec().decode(new JsonReader(document.toJson()), DecoderContext.builder().build()),
                     document);
    }

    @Test
    public void toJsonShouldRespectDefaultJsonWriterSettings() {
        StringWriter writer = new StringWriter();
        new BsonDocumentCodec().encode(new JsonWriter(writer), document, EncoderContext.builder().build());
        assertEquals(writer.toString(), document.toJson());
    }

    @Test
    public void toJsonShouldRespectJsonWriterSettings() {
        StringWriter writer = new StringWriter();
        JsonWriterSettings settings = JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build();
        new BsonDocumentCodec().encode(new JsonWriter(writer, settings), document, EncoderContext.builder().build());
        assertEquals(writer.toString(), document.toJson(settings));
    }

    @Test
    public void toStringShouldEqualToJson() {
        assertEquals(document.toJson(), document.toString());
    }

    @Test
    public void shouldParseJson() {
        assertEquals(new BsonDocument("a", new BsonInt32(1)), BsonDocument.parse("{\"a\" : 1}"));
    }
}
