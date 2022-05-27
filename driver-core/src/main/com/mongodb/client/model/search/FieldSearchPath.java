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
package com.mongodb.client.model.search;

import com.mongodb.annotations.Beta;
import com.mongodb.annotations.Evolving;
import org.bson.conversions.Bson;

import static com.mongodb.internal.client.model.Util.SEARCH_PATH_VALUE_KEY;

/**
 * @see SearchPath#fieldPath(String)
 * @since 4.7
 */
@Evolving
@Beta(Beta.Reason.CLIENT)
public interface FieldSearchPath extends SearchPath {
    /**
     * Creates a new {@link FieldSearchPath} with the name of the alternate analyzer specified.
     *
     * @param analyzerName The name of the alternate analyzer.
     * @return A new {@link FieldSearchPath}.
     */
    FieldSearchPath multi(String analyzerName);

    /**
     * Returns the name of the field represented by this path.
     * <p>
     * This method may be useful when using the {@code of} methods, e.g., {@link SearchScore#of(Bson)}.
     * Depending on the syntax of the document being constructed,
     * it may be required to use the method {@link SearchPath#toBsonValue()} instead.</p>
     *
     * @return A {@link String} {@linkplain String#equals(Object) equal} to the one used to {@linkplain SearchPath#fieldPath(String) create}
     * this path.
     * @see SearchPath#toBsonValue()
     */
    default String toValue() {
        return toBsonDocument().getString(SEARCH_PATH_VALUE_KEY).getValue();
    }
}
