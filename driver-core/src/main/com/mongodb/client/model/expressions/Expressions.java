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

package com.mongodb.client.model.expressions;

import com.mongodb.annotations.Beta;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class Expressions {

    private Expressions() {}

    @Beta(Beta.Reason.CLIENT) // TODO how to communicate that this is a soon-to-be deprecated shim?
    public static final DocumentExpression CURRENT = new MqlExpression<>((cr) -> new BsonString("$$CURRENT"));

    public static BooleanExpression ofBoolean(final boolean of) {
        // we intentionally disallow ofBoolean(null)
        return new MqlExpression<>((codecRegistry) -> new BsonBoolean(of));
    }

    public static BooleanExpression ofTrue() {
        return ofBoolean(true);
    }

    public static BooleanExpression ofFalse() {
        return ofBoolean(false);
    }

    public static IntegerExpression ofInteger(final int of) {
        return new MqlExpression<>((codecRegistry) -> new BsonInt32(of));
    }

    public static StringExpression ofString(final String of) {
        return new MqlExpression<>((codecRegistry) -> new BsonString(of));
    }

    public static ArrayExpression<IntegerExpression> ofIntegerArray(final int... ofIntegerArray) {
        List<BsonValue> array = Arrays.stream(ofIntegerArray)
                .mapToObj(BsonInt32::new)
                .collect(Collectors.toList());
        return new MqlExpression<>((cr) -> new BsonArray(array));
    }

    public static DocumentExpression ofDocument(final BsonDocument document) {
        // All documents are wrapped in a $literal. It is possible to check
        // for empty docs (see https://jira.mongodb.org/browse/SERVER-46422)
        // and to traverse for $$, but doing so would be unsafe and brittle.
        return new MqlExpression<>((cr) -> document == null
                ? new BsonNull()
                : new BsonDocument("$literal", document));
    }

    public static <R extends Expression> R ofNull() {
        // TODO Every type can be a null value.
        return new MqlExpression<>((cr) -> new BsonNull()).assertImplementsAllExpressions();
    }
}
