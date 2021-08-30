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

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.search.SearchPath.path;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SearchOperatorsTest {
    @Test
    public void testTextSearchOperator() {
        TextSearchOperator operator = SearchOperators.text("query1", path("field1"));

        assertEquals(new BsonDocument("text",
                        new BsonDocument("query", new BsonString("query1"))
                                .append("path", new BsonString("field1"))),
                operator.toBsonDocument());

        operator = operator.index("index1")
                .score(SearchScore.boost(1.5));

        assertEquals(new BsonDocument("index", new BsonString("index1"))
                        .append("text", new BsonDocument("query", new BsonString("query1"))
                                .append("path", new BsonString("field1"))
                                .append("score", new BsonDocument("boost", new BsonDocument("value", new BsonDouble(1.5))))),
                operator.toBsonDocument());
    }

    @Test
    public void testEqualsSearchOperator() {
        EqualsSearchOperator operator = SearchOperators.equal(true, path("field1"));

        assertEquals(new BsonDocument("equals",
                        new BsonDocument("value", BsonBoolean.TRUE).append("path", new BsonString("field1"))),
                operator.toBsonDocument());

        operator = operator.index("index1");

        assertEquals(new BsonDocument("index", new BsonString("index1"))
                        .append("equals", new BsonDocument("value", BsonBoolean.TRUE).append("path", new BsonString("field1"))),
                operator.toBsonDocument());
    }
}
