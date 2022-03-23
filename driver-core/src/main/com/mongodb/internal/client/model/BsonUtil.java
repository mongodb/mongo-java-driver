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

import com.mongodb.client.model.ToBsonField;
import com.mongodb.client.model.ToBsonValue;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.Iterator;

import static com.mongodb.assertions.Assertions.fail;

public final class BsonUtil {
    /**
     * Returns a {@link BsonDocument} constructed from the specified {@code fields}.
     *
     * @param fields Document fields.
     * @return The requested {@link BsonDocument}.
     */
    public static BsonDocument toBsonDocument(final Iterator<? extends ToBsonField> fields) {
        BsonDocument result = new BsonDocument();
        while (fields.hasNext()) {
            fields.next().appendTo(result);
        }
        return result;
    }

    /**
     * If {@code nonEmptyValues} has exactly one element, then returns the result of its {@link ToBsonValue#toBsonValue()},
     * otherwise returns a {@link BsonArray} of such results.
     *
     * @param nonEmptyValues One or more values to be converted.
     * @return A single {@link BsonValue} representing the specified values.
     */
    public static BsonValue toBsonValueOrArray(final Iterator<? extends ToBsonValue> nonEmptyValues) {
        BsonValue firstValue = nonEmptyValues.next().toBsonValue();
        if (nonEmptyValues.hasNext()) {
            BsonArray bsonArray = new BsonArray();
            bsonArray.add(firstValue);
            while (nonEmptyValues.hasNext()) {
                bsonArray.add(nonEmptyValues.next().toBsonValue());
            }
            return bsonArray;
        } else {
            return firstValue;
        }
    }

    private BsonUtil() {
        throw fail();
    }
}
