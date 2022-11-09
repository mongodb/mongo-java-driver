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

import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.json.JsonReader;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

// Don't convert to Spock, as Groovy intercepts equals/hashCode methods that we are trying to test
public class DocumentTest {
    private final Document emptyDocument = new Document();
    private final Document document = new Document()
            .append("a", 1)
            .append("b", 2)
            .append("c", new Document("x", true))
            .append("d", asList(new Document("y", false), 1));

    private final Document customDocument = new Document("database", new Name("MongoDB"));
    private final CodecRegistry customRegistry = fromRegistries(fromCodecs(new NameCodec()),
            fromProviders(new DocumentCodecProvider(), new ValueCodecProvider(), new BsonValueCodecProvider()));
    private final DocumentCodec customDocumentCodec = new DocumentCodec(customRegistry, new BsonTypeClassMap());

    @Test
    public void shouldBeEqualToItself() {
        assertEquals(emptyDocument, emptyDocument);
        assertEquals(document, document);
    }

    @Test
    public void shouldNotBeEqualToDifferentBsonDocument() {
        // expect
        assertNotEquals(emptyDocument, document);
    }

    @Test
    public void shouldHaveSameHashCodeAsEquivalentBsonDocument() {
        assertEquals(emptyDocument.hashCode(), new BsonDocument().hashCode());
    }

    @Test
    public void toJsonShouldReturnEquivalent() {
        assertEquals(new DocumentCodec().decode(new JsonReader(document.toJson()), DecoderContext.builder().build()),
                     document);
    }

    // Test to ensure that toJson does not reorder _id field
    @Test
    public void toJsonShouldNotReorderIdField() {
        // given
        Document d = new Document().append("x", 1)
                .append("y", Collections.singletonList("one"))
                .append("_id", "1");
        assertEquals("{\"x\": 1, \"y\": [\"one\"], \"_id\": \"1\"}", d.toJson());
    }

    // Test in Java to make sure none of the casts result in compiler warnings or class cast exceptions
    @Test
    public void shouldGetWithDefaultValue() {
        // given
        Document d = new Document("x", 1)
                .append("y", Collections.singletonList("one"))
                .append("z", "foo");

        // when the key is found
        int x = d.get("x", 2);
        List<String> y = d.get("y", asList("three", "four"));
        String z = d.get("z", "bar");

        // then it returns the value
        assertEquals(1, x);
        assertEquals(asList("one"), y);
        assertEquals("foo", z);

        // when the key is not found
        int x2 = d.get("x2", 2);
        List<String> y2 = d.get("y2", asList("three", "four"));
        String z2 = d.get("z2", "bar");

        // then it returns the default value
        assertEquals(2, x2);
        assertEquals(asList("three", "four"), y2);
        assertEquals("bar", z2);
    }

    @Test
    public void toJsonShouldTakeACustomDocumentCodec() {

        try {
            customDocument.toJson();
            fail("Should fail due to custom type");
        } catch (CodecConfigurationException e) {
            // noop
        }

        assertEquals("{\"database\": {\"name\": \"MongoDB\"}}", customDocument.toJson(customDocumentCodec));
    }

    @Test
    public void toBsonDocumentShouldCreateBsonDocument() {
        BsonDocument expected = new BsonDocument()
                .append("a", new BsonInt32(1))
                .append("b", new BsonInt32(2))
                .append("c", new BsonDocument("x", BsonBoolean.TRUE))
                .append("d", new BsonArray(asList(new BsonDocument("y", BsonBoolean.FALSE), new BsonInt32(1))));

        assertEquals(expected, document.toBsonDocument(BsonDocument.class, Bson.DEFAULT_CODEC_REGISTRY));
        assertEquals(expected, document.toBsonDocument());
    }

    @Test
    public void toJsonShouldRenderUuidAsStandard() {
        UUID uuid = UUID.randomUUID();
        Document doc = new Document("_id", uuid);

        String json = doc.toJson();
        assertEquals(new BsonDocument("_id", new BsonBinary(uuid)), BsonDocument.parse(json));
    }

    public class Name {
        private final String name;

        public Name(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    class NameCodec implements CollectibleCodec<Name> {

        @Override
        public void encode(final BsonWriter writer, final Name n, final EncoderContext encoderContext) {
            writer.writeStartDocument();
            writer.writeString("name", n.getName());
            writer.writeEndDocument();
        }

        @Override
        public Name decode(final BsonReader reader, final DecoderContext decoderContext) {
            reader.readStartDocument();
            String name = reader.readString("_id");
            reader.readEndDocument();
            return new Name(name);
        }

        @Override
        public Class<Name> getEncoderClass() {
            return Name.class;
        }

        @Override
        public boolean documentHasId(final Name document) {
            return false;
        }

        @Override
        public BsonObjectId getDocumentId(final Name document) {
            return null;
        }

        @Override
        public Name generateIdIfAbsentFromDocument(final Name document) {
            return document;
        }
    }

}
