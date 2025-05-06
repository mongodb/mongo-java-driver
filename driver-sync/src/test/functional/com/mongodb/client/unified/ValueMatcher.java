/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.unified;

import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

final class ValueMatcher {
    private final Entities entities;
    private final AssertionContext context;
    private static final List<String> NUMBER_TYPES = asList("int", "long", "double", "decimal");

    ValueMatcher(final Entities entities, final AssertionContext context) {
        this.entities = entities;
        this.context = context;
    }

    public void assertValuesMatch(final BsonValue expected, final BsonValue actual) {
        assertValuesMatch(expected, actual, null, -1);
    }

    private void assertValuesMatch(final BsonValue initialExpected, @Nullable final BsonValue actual,
                                   @Nullable final String keyContext, final int arrayPositionContext) {
        BsonValue expected = initialExpected;
        context.push(ContextElement.ofValueMatcher(expected, actual, keyContext, arrayPositionContext));

        try {
            if (initialExpected.isDocument() && initialExpected.asDocument().size() == 1
                    && initialExpected.asDocument().getFirstKey().startsWith("$$")) {
                BsonDocument expectedDocument = initialExpected.asDocument();

                switch (expectedDocument.getFirstKey()) {
                    case "$$exists":
                        if (expectedDocument.getBoolean("$$exists").getValue()) {
                            assertNotNull(context.getMessage("Actual document must contain key " + keyContext), actual);
                        } else {
                            assertNull(context.getMessage("Actual document must not contain key " + keyContext), actual);
                        }
                        return;
                    case "$$unsetOrMatches":
                        if (actual == null) {
                            return;
                        }
                        expected = expectedDocument.get("$$unsetOrMatches");
                        break;
                    case "$$type":
                        assertExpectedType(actual, expectedDocument.get("$$type"));
                        return;
                    case "$$matchesHexBytes":
                        expected = expectedDocument.getString("$$matchesHexBytes");
                        break;
                    case "$$matchAsRoot":
                        expected = expectedDocument.getDocument("$$matchAsRoot");
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported special operator: " + expectedDocument.getFirstKey());
                }
            }

            if (expected.isDocument()) {
                BsonDocument expectedDocument = expected.asDocument();
                assertTrue(context.getMessage("Actual value must be a document but is " + actual.getBsonType()), actual.isDocument());
                BsonDocument actualDocument = actual.asDocument();
                expectedDocument.forEach((key, value) -> {
                    BsonValue actualValue = actualDocument.get(key);
                    if (value.isDocument() && value.asDocument().size() == 1 && value.asDocument().getFirstKey().startsWith("$$")) {
                        switch (value.asDocument().getFirstKey()) {
                            case "$$exists":
                                if (value.asDocument().getBoolean("$$exists").getValue()) {
                                    assertTrue(context.getMessage("Actual document must contain key " + key),
                                            actualDocument.containsKey(key));
                                } else {
                                    assertFalse(context.getMessage("Actual document must not contain key " + key),
                                            actualDocument.containsKey(key));
                                }
                                return;
                            case "$$type":
                                assertTrue(context.getMessage("Actual document must contain key " + key), actualDocument.containsKey(key));
                                assertExpectedType(actualDocument.get(key), value.asDocument().get("$$type"));
                                return;
                            case "$$unsetOrMatches":
                                if (actualValue == null) {
                                    return;
                                }
                                value = value.asDocument().get("$$unsetOrMatches");
                                break;
                            case "$$matchesEntity":
                                value = entities.getResult(value.asDocument().getString("$$matchesEntity").getValue());
                                break;
                            case "$$matchesHexBytes":
                                value = value.asDocument().getString("$$matchesHexBytes");
                                break;
                            case "$$sessionLsid":
                                value = entities.getSessionIdentifier(value.asDocument().getString("$$sessionLsid").getValue());
                                break;
                            case "$$matchAsDocument":
                                actualValue = BsonDocument.parse(actualValue.asString().getValue());
                                value = value.asDocument().getDocument("$$matchAsDocument");
                                break;
                            case "$$lte":
                                value = value.asDocument().getNumber("$$lte");
                                assertTrue(actualValue.asNumber().longValue() <= value.asNumber().longValue());
                                return;
                            default:
                                throw new UnsupportedOperationException("Unsupported special operator: " + value.asDocument().getFirstKey());
                        }
                    }

                    assertNotNull(context.getMessage("Actual document must contain key " + key), actualValue);
                    assertValuesMatch(value, actualValue, key, -1);
                });
            } else if (expected.isArray()) {
                assertTrue(context.getMessage("Actual value must be an array but is " + actual.getBsonType()), actual.isArray());
                assertEquals(context.getMessage("Arrays must be the same size"), expected.asArray().size(), actual.asArray().size());
                for (int i = 0; i < expected.asArray().size(); i++) {
                    assertValuesMatch(expected.asArray().get(i), actual.asArray().get(i), keyContext, i);
                }
            } else if (expected.isNumber()) {
                assertTrue(context.getMessage("Expected a number"), actual.isNumber());
                assertEquals(context.getMessage("Expected BSON numbers to be equal"),
                        expected.asNumber().doubleValue(), actual.asNumber().doubleValue(), 0.0);
            } else if (expected.isNull()) {
                assertTrue(context.getMessage("Expected BSON null"), actual == null || actual.isNull());
            } else {
                assertEquals(context.getMessage("Expected BSON types to be equal"), expected.getBsonType(), actual.getBsonType());
                assertEquals(context.getMessage("Expected BSON values to be equal"), expected, actual);
            }
        } finally {
            context.pop();
        }
    }

    private void assertExpectedType(final BsonValue actualValue, final BsonValue expectedTypes) {
        List<String> types;
        if (expectedTypes.isString()) {
            String expectedType = expectedTypes.asString().getValue();
            types = expectedType.equals("number") ? NUMBER_TYPES : singletonList(expectedType);
        } else if (expectedTypes.isArray()) {
            types = expectedTypes.asArray().stream().map(type -> type.asString().getValue()).collect(Collectors.toList());
        } else {
            throw new UnsupportedOperationException("Unsupported type for $$type value");
        }
        assertTrue(context.getMessage("Expected BSON type to be one of " + types + " but was "
                        + asTypeString(actualValue.getBsonType())),
                types.contains(asTypeString(actualValue.getBsonType())));
    }

    private static String asTypeString(final BsonType bsonType) {
        switch (bsonType) {
            case DOUBLE:
                return "double";
            case STRING:
                return "string";
            case DOCUMENT:
                return "object";
            case ARRAY:
                return "array";
            case BINARY:
                return "binData";
            case OBJECT_ID:
                return "objectId";
            case BOOLEAN:
                return "bool";
            case DATE_TIME:
                return "date";
            case NULL:
                return "null";
            case REGULAR_EXPRESSION:
                return "regex";
            case INT32:
                return "int";
            case TIMESTAMP:
                return "timestamp";
            case INT64:
                return "long";
            case DECIMAL128:
                return "decimal";
            default:
                throw new UnsupportedOperationException("Unsupported bson type conversion to string: " + bsonType);
        }
    }
}
