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

import com.mongodb.client.model.Field;
import com.mongodb.client.model.OperationTest;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonReader;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DecoderContext;
import org.bson.conversions.Bson;
import org.bson.json.JsonReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.model.Aggregates.addFields;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * This test file groups expressions of each type under one test method. Each of
 * these methods begins by showing how to express literals of the relevant type.
 * The ensuing assertions then show how to express the computation in Java (when
 * reasonable), then using the API under test, and then in MQL.
 */
@SuppressWarnings({
        // for demonstration:
        "PointlessBooleanExpression",
        "ConstantConditions",
})
public class ExpressionsFunctionalTest extends OperationTest {

    @Test
    public void booleanExpressionsTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#boolean-expression-operators
        // (Complete as of 6.0)

        // literals
        BooleanExpression tru = Expressions.ofTrue();
        BooleanExpression fal = Expressions.ofFalse();
        assertExpression(true, tru, "true");
        assertExpression(false, fal, "false");
        assertTrue(tru instanceof BooleanExpression);

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/or/
        assertExpression(true || false, tru.or(fal), "{'$or': [true, false]}");
        assertExpression(false || true, fal.or(tru), "{'$or': [false, true]}");

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/and/
        assertExpression(true && false, tru.and(fal), "{'$and': [true, false]}");
        assertExpression(false && true, fal.and(tru), "{'$and': [false, true]}");

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/not/
        assertExpression(!true, tru.not(), "{'$not': true}");
        assertExpression(!false, fal.not(), "{'$not': false}");

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/cond/
        StringExpression abc = Expressions.ofString("abc");
        StringExpression xyz = Expressions.ofString("xyz");
        assertExpression(
                true && false ? "abc" : "xyz",
                tru.and(fal).cond(abc, xyz),
                "{'$cond': [{'$and': [true, false]}, 'abc', 'xyz']}");
        assertExpression(
                true || false ? "abc" : "xyz",
                tru.or(fal).cond(abc, xyz),
                "{'$cond': [{'$or': [true, false]}, 'abc', 'xyz']}");
    }

    @BeforeEach
    public void setUp() {
        getCollectionHelper().drop();
    }

    @AfterEach
    public void tearDown() {
        getCollectionHelper().drop();
    }

    private void assertExpression(final Object result, final Expression exp, final String string) {
        assertEval(result, exp);

        BsonValue expressionValue = ((MqlExpression) exp).toBsonValue(fromProviders(new BsonValueCodecProvider()));
        BsonValue bsonValue = new BsonDocumentFragmentCodec().readValue(
                new JsonReader(string),
                DecoderContext.builder().build());
        assertEquals(bsonValue, expressionValue, expressionValue.toString().replace("\"", "'"));
    }

    private void assertEval(final Object expected, final Expression toEvaluate) {
        Bson addFieldsStage = addFields(new Field<>("val", toEvaluate));
        List<Bson> stages = new ArrayList<>();
        stages.add(addFieldsStage);
        List<BsonDocument> results;
        if (getCollectionHelper().count() == 0) {
            // TODO: replace documents stage for older versions of the server?
            assumeTrue(serverVersionAtLeast(5, 1));
            BsonDocument document = new BsonDocument("val", new BsonString("#invalid string#"));
            Bson documentsStage = new BsonDocument("$documents", new BsonArray(Arrays.asList(document)));
            stages.add(0, documentsStage);
            results = getCollectionHelper().aggregateDb(stages);
        } else {
            results = getCollectionHelper().aggregate(stages);
        }
        BsonValue evaluated = results.get(0).get("val");
        assertEquals(new Document("val", expected).toBsonDocument().get("val"), evaluated);
    }

    private static class BsonDocumentFragmentCodec extends BsonDocumentCodec {
        public BsonValue readValue(final BsonReader reader, final DecoderContext decoderContext) {
            reader.readBsonType();
            return super.readValue(reader, decoderContext);
        }
    }
}

