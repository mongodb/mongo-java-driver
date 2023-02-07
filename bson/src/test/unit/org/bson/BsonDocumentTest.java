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

package org.bson;

import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonMode;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

// Don't convert to Spock, as Groovy intercepts equals/hashCode methods that we are trying to test
public class BsonDocumentTest {
    private final BsonDocument emptyDocument = new BsonDocument();
    private final BsonDocument emptyRawDocument = new RawBsonDocument(emptyDocument, new BsonDocumentCodec());
    private final BsonDocument document = new BsonDocument()
                                    .append("a", new BsonInt32(1))
                                    .append("b", new BsonInt32(2))
                                    .append("c", new BsonDocument("x", BsonBoolean.TRUE))
                                    .append("d", new BsonArray(Arrays.asList(new BsonDocument("y",
                                                                                                         BsonBoolean.FALSE),
                                                                                        new BsonInt32(1))));

    private final BsonDocument rawDocument = new RawBsonDocument(document, new BsonDocumentCodec());

    @Test
    public void shouldBeEqualToItself() {
        assertEquals(emptyDocument, emptyDocument);
        assertEquals(document, document);
    }

    @Test
    public void shouldBeEqualToEquivalentBsonDocument() {
        assertEquals(emptyDocument, emptyRawDocument);
        assertEquals(document, rawDocument);
        assertEquals(emptyRawDocument, emptyDocument);
        assertEquals(rawDocument, document);
    }

    @Test
    public void shouldNotBeEqualToDifferentBsonDocument() {
        // expect
        assertNotEquals(emptyDocument, document);
        assertNotEquals(document, emptyRawDocument);
        assertNotEquals(document, emptyRawDocument);
        assertNotEquals(emptyRawDocument, document);
        assertNotEquals(rawDocument, emptyDocument);
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

    @Test
    public void cloneIsDeepCopyAndMutable() {
        Consumer<BsonDocument> assertCloneDeepCopyMutable = original -> {
            BsonDocument clone = original.clone();
            assertNotSame(original, clone);
            assertEquals(original, clone);
            // check that mutating `clone` does not mutate `original`
            clone.getDocument("k1").put("k2", new BsonString("clone"));
            assertEquals(new BsonString("clone"), clone.getDocument("k1").get("k2"));
            assertEquals(BsonNull.VALUE, original.getDocument("k1").get("k2"));
            // check that mutating `original` (if it is mutable) does not mutate `clone`
            if (!(original instanceof RawBsonDocument)) {
                original.put("k1", new BsonDocument("k2", new BsonString("original")));
                assertEquals(new BsonString("original"), original.getDocument("k1").get("k2"));
                assertEquals(new BsonString("clone"), clone.getDocument("k1").get("k2"));
            }
        };
        assertAll(
                () -> assertCloneDeepCopyMutable.accept(new BsonDocument("k1", new BsonDocument("k2", BsonNull.VALUE))),
                () -> assertCloneDeepCopyMutable.accept(new BsonDocument("k1", RawBsonDocument.parse("{'k2': null}"))),
                () -> assertCloneDeepCopyMutable.accept(RawBsonDocument.parse("{'k1': {'k2': null}}"))
        );
    }
}
