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
package org.bson.internal;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonValue;
import org.bson.RawBsonArray;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

final class UtilTest {
    @Test
    public void mutableDeepCopy() {
        Entry<String, BsonBinary> originalBsonBinaryEntry = new SimpleImmutableEntry<>(
                "bsonBinary",
                new BsonBinary("bsonBinary".getBytes(StandardCharsets.UTF_8))
        );
        Entry<String, BsonJavaScriptWithScope> originalBsonJavaScriptWithScopeEntry = new SimpleImmutableEntry<>(
                "bsonJavaScriptWithScopeEntry",
                new BsonJavaScriptWithScope("\"use strict\";", new BsonDocument())
        );
        Entry<String, RawBsonDocument> originalRawBsonDocumentEntry = new SimpleImmutableEntry<>(
                "rawBsonDocument",
                RawBsonDocument.parse("{rawBsonDocument: 'rawBsonDocument_value'}")
        );
        Entry<String, BsonDocumentWrapper<RawBsonDocument>> originalBsonDocumentWrapperEntry = new SimpleImmutableEntry<>(
                "bsonDocumentWrapper",
                new BsonDocumentWrapper<>(originalRawBsonDocumentEntry.getValue(), Bson.DEFAULT_CODEC_REGISTRY.get(RawBsonDocument.class))
        );
        Entry<String, BsonDocument> originalBsonDocumentEntry = new SimpleImmutableEntry<>(
                "bsonDocument",
                new BsonDocument()
                        .append(originalBsonBinaryEntry.getKey(), originalBsonBinaryEntry.getValue())
                        .append(originalBsonJavaScriptWithScopeEntry.getKey(), originalBsonJavaScriptWithScopeEntry.getValue())
                        .append(originalRawBsonDocumentEntry.getKey(), originalRawBsonDocumentEntry.getValue())
                        .append(originalBsonDocumentWrapperEntry.getKey(), originalBsonDocumentWrapperEntry.getValue())
        );
        Entry<String, BsonArray> originalBsonArrayEntry = new SimpleImmutableEntry<>(
                "bsonArray",
                new BsonArray(singletonList(new BsonArray()))
        );
        Entry<String, RawBsonArray> originalRawBsonArrayEntry = new SimpleImmutableEntry<>(
                "rawBsonArray",
                rawBsonArray(
                        originalBsonBinaryEntry.getValue(),
                        originalBsonJavaScriptWithScopeEntry.getValue(),
                        originalRawBsonDocumentEntry.getValue(),
                        originalBsonDocumentWrapperEntry.getValue(),
                        originalBsonDocumentEntry.getValue(),
                        originalBsonArrayEntry.getValue())
        );
        BsonDocument original = new BsonDocument()
                .append(originalBsonBinaryEntry.getKey(), originalBsonBinaryEntry.getValue())
                .append(originalBsonJavaScriptWithScopeEntry.getKey(), originalBsonJavaScriptWithScopeEntry.getValue())
                .append(originalRawBsonDocumentEntry.getKey(), originalRawBsonDocumentEntry.getValue())
                .append(originalBsonDocumentWrapperEntry.getKey(), originalBsonDocumentWrapperEntry.getValue())
                .append(originalBsonDocumentEntry.getKey(), originalBsonDocumentEntry.getValue())
                .append(originalBsonArrayEntry.getKey(), originalBsonArrayEntry.getValue())
                .append(originalRawBsonArrayEntry.getKey(), originalRawBsonArrayEntry.getValue());
        BsonDocument copy = Util.mutableDeepCopy(original);
        assertEqualNotSameAndMutable(original, copy);
        original.forEach((key, value) -> assertEqualNotSameAndMutable(value, copy.get(key)));
        // check nested document
        String nestedDocumentKey = originalBsonDocumentEntry.getKey();
        BsonDocument originalNestedDocument = original.getDocument(nestedDocumentKey);
        BsonDocument copyNestedDocument = copy.getDocument(nestedDocumentKey);
        assertEqualNotSameAndMutable(originalNestedDocument, copyNestedDocument);
        originalNestedDocument.forEach((key, value) -> assertEqualNotSameAndMutable(value, copyNestedDocument.get(key)));
        // check nested array
        String nestedArrayKey = originalRawBsonArrayEntry.getKey();
        BsonArray originalNestedArray = original.getArray(nestedArrayKey);
        BsonArray copyNestedArray = copy.getArray(nestedArrayKey);
        assertEqualNotSameAndMutable(originalNestedArray, copyNestedArray);
        for (int i = 0; i < originalNestedArray.size(); i++) {
            assertEqualNotSameAndMutable(originalNestedArray.get(i), copyNestedArray.get(i));
        }
    }

    private static RawBsonArray rawBsonArray(final BsonValue... elements) {
        return (RawBsonArray) new RawBsonDocument(
                new BsonDocument("a", new BsonArray(asList(elements))), Bson.DEFAULT_CODEC_REGISTRY.get(BsonDocument.class))
                .get("a");
    }

    private static void assertEqualNotSameAndMutable(final Object expected, final Object actual) {
        assertEquals(expected, actual);
        assertNotSame(expected, actual);
        Class<?> actualClass = actual.getClass();
        if (expected instanceof BsonDocument) {
            assertEquals(BsonDocument.class, actualClass);
        } else if (expected instanceof BsonArray) {
            assertEquals(BsonArray.class, actualClass);
        } else if (expected instanceof BsonBinary) {
            assertEquals(BsonBinary.class, actualClass);
        } else if (expected instanceof BsonJavaScriptWithScope) {
            assertEquals(BsonJavaScriptWithScope.class, actualClass);
        } else {
            org.bson.assertions.Assertions.fail("Unexpected " + expected.getClass().toString());
        }
    }

    private UtilTest() {
    }
}
