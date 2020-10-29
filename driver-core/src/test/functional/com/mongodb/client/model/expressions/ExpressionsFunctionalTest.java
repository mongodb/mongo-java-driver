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

import com.mongodb.MongoNamespace;
import com.mongodb.client.test.CollectionHelper;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.expressions.Expressions.add;
import static com.mongodb.client.model.expressions.Expressions.array;
import static com.mongodb.client.model.expressions.Expressions.branch;
import static com.mongodb.client.model.expressions.Expressions.currentRef;
import static com.mongodb.client.model.expressions.Expressions.document;
import static com.mongodb.client.model.expressions.Expressions.fieldPath;
import static com.mongodb.client.model.expressions.Expressions.gte;
import static com.mongodb.client.model.expressions.Expressions.literal;
import static com.mongodb.client.model.expressions.Expressions.of;
import static com.mongodb.client.model.expressions.Expressions.ref;
import static com.mongodb.client.model.expressions.Expressions.rootRef;
import static com.mongodb.client.model.expressions.Expressions.switchExpr;
import static com.mongodb.client.model.expressions.Expressions.unparsedLiteral;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.bson.BsonDocument.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExpressionsFunctionalTest {

    private CollectionHelper<BsonDocument> helper;

    @Before
    public void setUp() {
        helper = new CollectionHelper<>(new BsonDocumentCodec(),
                new MongoNamespace(getDefaultDatabaseName(), getClass().getName()));
        helper.insertDocuments(new BsonDocument("_id", new BsonInt32(1)));
    }

    @After
    public void tearDown() {
        helper.drop();
    }

    @Test
    public void addTest() {
        assertEquals(3, eval(add(literal(1), literal(2))).asNumber().intValue());
    }

    @Test
    public void gteTest() {
        assertTrue(eval(gte(literal(7), literal(6))).asBoolean().getValue());
        assertTrue(eval(gte(literal(7), literal(7))).asBoolean().getValue());
    }

    @Test
    public void switchExprTest() {
        assertEquals(2,
                eval(switchExpr(
                        branch(literal(false), literal(1)),
                        branch(literal(true), literal(2)))
                ).asNumber().intValue());
        assertEquals(3,
                eval(switchExpr(
                        branch(literal(false), literal(1)),
                        branch(literal(false), literal(2)))
                        .defaultExpr(literal(3))
                ).asNumber().intValue());
    }

    @Test
    public void literalTest() {
        assertEquals("str", eval(literal("str")).asString().getValue());
        assertEquals(BsonNull.VALUE, eval(literal((Object) null)));
    }

    @Test
    public void unparsedLiteralTest() {
        assertTrue(eval(unparsedLiteral(true)).asBoolean().getValue());
        assertEquals(5, eval(unparsedLiteral(5)).asNumber().intValue());
        assertEquals("$$path", eval(unparsedLiteral("$$path")).asString().getValue());
    }

    @Test
    public void documentTest() {
        Map<String, Expression> map = new HashMap<>();
        map.put("key", add(literal(1), literal(2)));
        assertEquals(parse("{key: 3}"), eval(document(map)));
    }

    @Test
    public void arrayTest() {
        assertEquals(
                new BsonArray(asList(new BsonInt32(3), new BsonInt32(10))),
                eval(array(asList(add(literal(2), literal(1)), add(literal(7), literal(3))))));
    }

    @Test
    public void fieldPathTest() {
        assertEquals(1, eval(fieldPath("_id")).asNumber().intValue());
    }

    @Test
    public void currentRefTest() {
        assertEquals(parse("{_id: 1}"), eval(currentRef()));
        assertEquals(1, eval(currentRef().fieldPath("_id")).asNumber().intValue());
    }

    @Test
    public void rootRefTest() {
        assertEquals(parse("{_id: 1}"), eval(rootRef()));
        assertEquals(1, eval(rootRef().fieldPath("_id")).asNumber().intValue());
    }

    @Test
    public void refTest() {
        assertEquals(parse("{_id: 1}"), eval(ref("ROOT")));
        assertEquals(1, eval(ref("ROOT").fieldPath("_id")).asNumber().intValue());
    }

    @Test
    public void ofTest() {
        assertTrue(eval(of(parse("{$and: [true, true]}"))).asBoolean().getValue());
    }

    private BsonValue eval(final Expression expression) {
        return helper.aggregate(singletonList(
                project(computed("val", expression.toBsonValue(getDefaultCodecRegistry())))))
                .get(0).get("val");
    }
}
