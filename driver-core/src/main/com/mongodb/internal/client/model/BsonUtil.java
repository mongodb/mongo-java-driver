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

import com.mongodb.client.model.search.SearchPath;
import org.bson.BsonArray;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNumber;
import org.bson.BsonValue;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.mongodb.assertions.Assertions.fail;

public final class BsonUtil {
    public static final String SEARCH_PATH_VALUE_KEY = "value";

    /**
     * If {@code nonEmptyPaths} has exactly one element, then returns the result of {@link SearchPath#toBsonValue()},
     * otherwise returns a {@link BsonArray} of such results.
     *
     * @param nonEmptyPaths One or more {@link SearchPath} to convert.
     * @return A single {@link BsonValue} representing the specified paths.
     */
    public static BsonValue toBsonValue(final Iterator<? extends SearchPath> nonEmptyPaths) {
        BsonValue firstPath = nonEmptyPaths.next().toBsonValue();
        if (nonEmptyPaths.hasNext()) {
            BsonArray bsonArray = new BsonArray();
            bsonArray.add(firstPath);
            while (nonEmptyPaths.hasNext()) {
                bsonArray.add(nonEmptyPaths.next().toBsonValue());
            }
            return bsonArray;
        } else {
            return firstPath;
        }
    }

    /**
     * Must handle the same {@link Number}s as those handled by {@code org.bson.BasicBSONEncoder.putNumber}.
     */
    public static BsonNumber toBsonNumber(final Number number) {
        if (number instanceof Integer || number instanceof Short || number instanceof Byte || number instanceof AtomicInteger) {
            return new BsonInt32(number.intValue());
        } else if (number instanceof Long || number instanceof AtomicLong) {
            return new BsonInt64(number.longValue());
        } else if (number instanceof Float || number instanceof Double) {
            return new BsonDouble(number.doubleValue());
        } else {
            throw new IllegalArgumentException("Can't serialize " + number.getClass());
        }
    }

    private BsonUtil() {
        throw fail();
    }
}
