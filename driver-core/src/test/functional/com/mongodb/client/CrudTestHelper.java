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

package com.mongodb.client;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import static org.junit.Assert.assertEquals;

public final class CrudTestHelper {

    public static void replaceTypeAssertionWithActual(final BsonDocument expected, final BsonDocument actual) {
        for (String key : expected.keySet()) {
            BsonValue value = expected.get(key);
            if (value.isDocument()) {
                BsonDocument valueDocument = value.asDocument();
                BsonValue actualValue = actual.get(key);
                if (valueDocument.size() == 1 && valueDocument.getFirstKey().equals("$$type")) {
                    String type = valueDocument.getString("$$type").getValue();
                    if (type.equals("binData")) {
                        assertEquals(BsonType.BINARY, actualValue.getBsonType());
                        expected.put(key, actualValue);
                    } else if (type.equals("long")) {
                        assertEquals(BsonType.INT64, actualValue.getBsonType());
                        expected.put(key, actualValue);
                    } else {
                        throw new UnsupportedOperationException("Unsupported type: " + type);
                    }
                } else if (actualValue != null && actualValue.isDocument()) {
                    replaceTypeAssertionWithActual(valueDocument, actualValue.asDocument());
                } else {
                    throw new RuntimeException(String.format("Expecting '%s' as actual value but found '%s' ", valueDocument, actualValue));
                }
            } else if (value.isArray()) {
                replaceTypeAssertionWithActual(value.asArray(), actual.get(key).asArray());
            }
        }
    }

    private static void replaceTypeAssertionWithActual(final BsonArray expected, final BsonArray actual) {
        for (int i = 0; i < expected.size(); i++) {
            BsonValue value = expected.get(i);
            if (value.isDocument()) {
                replaceTypeAssertionWithActual(value.asDocument(), actual.get(i).asDocument());
            } else if (value.isArray()) {
                replaceTypeAssertionWithActual(value.asArray(), actual.get(i).asArray());
            }
        }
    }
    private CrudTestHelper() {
    }
}
