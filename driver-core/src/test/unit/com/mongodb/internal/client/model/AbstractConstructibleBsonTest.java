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

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

final class AbstractConstructibleBsonTest {
    @Test
    void of() {
        BsonDocument doc = new BsonDocument("name", new BsonString("value"));
        AbstractConstructibleBson<?> constructible = AbstractConstructibleBson.of(doc);
        assertUnmodifiable(constructible);
        assertEquals(doc, constructible.toBsonDocument());
    }

    @Test
    void ofPreventsDoubleWrapping() {
        BsonDocument doc = new BsonDocument("name", new BsonString("value"));
        AbstractConstructibleBson<?> constructible = AbstractConstructibleBson.of(doc);
        assertUnmodifiable(constructible);
        AbstractConstructibleBson<?> constructible2 = AbstractConstructibleBson.of(constructible);
        assertUnmodifiable(constructible2);
        assertSame(constructible, constructible2);
    }

    @Test
    void newAppended() {
        AbstractConstructibleBson<?> constructible = AbstractConstructibleBson.of(new BsonDocument("name", new BsonString("value")));
        assertUnmodifiable(constructible);
        AbstractConstructibleBson<?> appendedConstructible = constructible.newAppended("name2", "value2");
        assertUnmodifiable(appendedConstructible);
        assertEquals(
                new BsonDocument("name", new BsonString("value")).append("name2", new BsonString("value2")),
                appendedConstructible.toBsonDocument());
    }

    @Test
    void emptyIsImmutable() {
        assertImmutable(AbstractConstructibleBson.EMPTY_IMMUTABLE);
        BsonDocument doc = new BsonDocument();
        assertImmutable(AbstractConstructibleBson.of(doc));
        assertEquals(new BsonDocument(), doc);
    }

    @Test
    void appendedCannotBeMutatedViaToBsonDocument() {
        appendedCannotBeMutatedViaToBsonDocument(new Document());
        appendedCannotBeMutatedViaToBsonDocument(new Document("appendedName", "appendedValue"));
    }

    @Test
    void tostring() {
        assertEquals(
                new Document(
                        "array", new BsonArray(asList(new BsonString("e1"), new BsonString("e2"))))
                        .append("double", 0.5)
                        .append("doc", new Document("i", 42))
                        .append("constructible", new Document("s", ""))
                        .toString(),
                AbstractConstructibleBson.of(
                        new BsonDocument("array", new BsonArray(asList(new BsonString("e1"), new BsonString("e2")))))
                        .newAppended("double", 0.5)
                        .newAppended("doc", new Document("i", 42))
                        .newAppended("constructible", AbstractConstructibleBson.of(AbstractConstructibleBson.of(new Document("s", ""))))
                        .toString());
    }

    private static void appendedCannotBeMutatedViaToBsonDocument(final Document appended) {
        String expected = appended.toBsonDocument().toJson();
        final class Constructible extends AbstractConstructibleBson<Constructible> {
            private Constructible(final Bson base, final Document appended) {
                super(base, appended);
            }

            @Override
            protected Constructible newSelf(final Bson base, final Document appended) {
                return new Constructible(base, appended);
            }
        }
        AbstractConstructibleBson<?> constructible = new Constructible(new Document("name", "value"), appended);
        // here we modify the document produced by `toBsonDocument` and check that it does not affect `appended`
        constructible.toBsonDocument().append("name2", new BsonString("value2"));
        assertEquals(expected, appended.toBsonDocument().toJson());
    }

    private static void assertImmutable(final AbstractConstructibleBson<?> constructible) {
        String expected = constructible.toBsonDocument().toJson();
        assertUnmodifiable(constructible);
        // here we modify the document produced by `toBsonDocument` and check that it does not affect `constructible`
        constructible.toBsonDocument().append("assertImmutableName", new BsonString("assertImmutableValue"));
        assertEquals(expected, constructible.toBsonDocument().toJson());
    }

    private static void assertUnmodifiable(final AbstractConstructibleBson<?> constructible) {
        String expected = constructible.toBsonDocument().toJson();
        constructible.newAppended("assertUnmodifiableName", "assertUnmodifiableValue");
        assertEquals(expected, constructible.toBsonDocument().toJson());
        constructible.newMutated(doc -> doc.append("assertUnmodifiableName", "assertUnmodifiableValue"));
        assertEquals(expected, constructible.toBsonDocument().toJson());
    }
}
