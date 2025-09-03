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

import com.mongodb.Function;
import com.mongodb.client.model.search.FieldSearchPath;
import com.mongodb.client.model.search.SearchPath;
import org.bson.BsonArray;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.Iterator;

import static com.mongodb.assertions.Assertions.fail;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class Util {
    public static final String SEARCH_PATH_VALUE_KEY = "value";

    /**
     * If {@code nonEmptyPaths} has exactly one element, then returns the result of {@link SearchPath#toBsonValue()},
     * otherwise returns a {@link BsonArray} of such results.
     *
     * @param nonEmptyPaths One or more {@link SearchPath} to convert.
     * @param valueOnly If {@code true}, then {@link FieldSearchPath#toValue()} is used when possible;
     * if {@code false}, then {@link SearchPath#toBsonValue()} is used.
     * @return A single {@link BsonValue} representing the specified paths.
     */
    public static BsonValue combineToBsonValue(final Iterator<? extends SearchPath> nonEmptyPaths, final boolean valueOnly) {
        Function<SearchPath, BsonValue> toBsonValueFunc = valueOnly
                ? path -> {
                    if (path instanceof FieldSearchPath) {
                        return new BsonString(((FieldSearchPath) path).toValue());
                    } else {
                        return path.toBsonValue();
                    }
                }
                : SearchPath::toBsonValue;
        BsonValue firstPath = toBsonValueFunc.apply(nonEmptyPaths.next());
        if (nonEmptyPaths.hasNext()) {
            BsonArray bsonArray = new BsonArray();
            bsonArray.add(firstPath);
            while (nonEmptyPaths.hasNext()) {
                bsonArray.add(toBsonValueFunc.apply(nonEmptyPaths.next()));
            }
            return bsonArray;
        } else {
            return firstPath;
        }
    }

    public static boolean sizeAtLeast(final Iterable<?> iterable, final int minInclusive) {
        Iterator<?> iter = iterable.iterator();
        int size = 0;
        while (size < minInclusive && iter.hasNext()) {
            iter.next();
            size++;
        }
        return size >= minInclusive;
    }

    private Util() {
        throw fail();
    }
}
