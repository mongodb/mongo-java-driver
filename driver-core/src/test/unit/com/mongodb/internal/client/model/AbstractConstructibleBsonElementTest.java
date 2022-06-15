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
package com.mongodb.internal.client.model;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

final class AbstractConstructibleBsonElementTest {
    @Test
    void of() {
        BsonDocument value = new BsonDocument("n", new BsonString("v"));
        AbstractConstructibleBsonElement<?> constructible = AbstractConstructibleBsonElement.of(
                new BsonDocument("name", value));
        assertUnmodifiable(constructible);
        assertEquals(new BsonDocument("name", value), constructible.toBsonDocument());
    }

    @Test
    void ofPreventsDoubleWrapping() {
        BsonDocument value = new BsonDocument("n", new BsonString("v"));
        AbstractConstructibleBsonElement<?> constructible = AbstractConstructibleBsonElement.of(
                new BsonDocument("name", value));
        assertUnmodifiable(constructible);
        AbstractConstructibleBsonElement<?> constructible2 = AbstractConstructibleBsonElement.of(constructible);
        assertUnmodifiable(constructible2);
        assertSame(constructible, constructible2);
    }

    @Test
    void nameConstructor() {
        final class Constructible extends AbstractConstructibleBsonElement<Constructible> {
            private Constructible(final String name) {
                super(name);
            }

            private Constructible(final Bson baseElement, final Bson appendedElementValue) {
                super(baseElement, appendedElementValue);
            }

            @Override
            protected Constructible newSelf(final Bson baseElement, final Bson appendedElementValue) {
                return new Constructible(baseElement, appendedElementValue);
            }
        }
        AbstractConstructibleBsonElement<?> constructible = new Constructible("name");
        assertUnmodifiable(constructible);
        assertEquals(new BsonDocument("name", new BsonDocument()), constructible.toBsonDocument());
    }

    @Test
    void nameValueConstructor() {
        final class Constructible extends AbstractConstructibleBsonElement<Constructible> {
            private Constructible(final String name, final Bson value) {
                super(name, value);
            }

            private Constructible(final Bson baseElement, final Bson appendedElementValue) {
                super(baseElement, appendedElementValue);
            }

            @Override
            protected Constructible newSelf(final Bson baseElement, final Bson appendedElementValue) {
                return new Constructible(baseElement, appendedElementValue);
            }
        }
        BsonDocument value = new BsonDocument("n", new BsonString("v"));
        AbstractConstructibleBsonElement<?> constructible = new Constructible("name", value);
        assertUnmodifiable(constructible);
        assertEquals(new BsonDocument("name", value), constructible.toBsonDocument());
    }

    @Test
    void newWithAppendedValue() {
        AbstractConstructibleBsonElement<?> constructible = AbstractConstructibleBsonElement.of(
                new BsonDocument("name", new BsonDocument("n", new BsonString("v"))));
        assertUnmodifiable(constructible);
        AbstractConstructibleBsonElement<?> appendedConstructible = constructible.newWithAppendedValue("n2", "v2");
        assertUnmodifiable(appendedConstructible);
        assertEquals(
                new BsonDocument("name", new BsonDocument("n", new BsonString("v")).append("n2", new BsonString("v2"))),
                appendedConstructible.toBsonDocument());
    }

    private static void assertUnmodifiable(final AbstractConstructibleBsonElement<?> constructible) {
        String expected = constructible.toBsonDocument().toJson();
        constructible.newWithAppendedValue("assertUnmodifiableN", "assertUnmodifiableV");
        assertEquals(expected, constructible.toBsonDocument().toJson());
        constructible.newWithMutatedValue(doc -> doc.append("assertUnmodifiableN", "assertUnmodifiableV"));
        assertEquals(expected, constructible.toBsonDocument().toJson());
    }
}
