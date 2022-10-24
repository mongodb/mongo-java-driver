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
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.json.JsonReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.Aggregates.addFields;
import static com.mongodb.client.model.expressions.Expressions.CURRENT;
import static com.mongodb.client.model.expressions.Expressions.ofNull;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This test file groups expressions of each type under one test method. Each of
 * these methods begins by showing how to express literals of the relevant type.
 * The ensuing assertions then show how to express the computation in Java (when
 * reasonable), then using the API under test, and then in MQL.
 */
@SuppressWarnings({
        // for demonstration:
        "PointlessBooleanExpression",
        "PointlessArithmeticExpression",
        "ConstantConditions",
        // for clarity:
        "Convert2MethodRef"})
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

        // fields
        getCollectionHelper().insertDocuments("[{ 'isTrue': true}]");
        assertExpression(
                true,
                CURRENT.getFieldBoolean("isTrue"),
                "{'$getField': {'input': '$$CURRENT', 'field': 'isTrue'}}");
        assertExpression(
                false,
                CURRENT.getFieldBoolean("isTrue").not(),
                "{'$not': {'$getField': {'input': '$$CURRENT', 'field': 'isTrue'}}}");
        getCollectionHelper().drop();

        // undefined behaviour
        getCollectionHelper().insertDocuments("[{'isNull': null, 'isInt0': 0, 'isInt1': 1}]");
        assertExpression(
                true,
                CURRENT.getFieldBoolean("isNull").not(),
                "{'$not': {'$getField': {'input': '$$CURRENT', 'field': 'isNull'}}}");
        assertExpression(
                true,
                CURRENT.getFieldBoolean("isInt0").not(),
                "{'$not': {'$getField': {'input': '$$CURRENT', 'field': 'isInt0'}}}");
        assertExpression(
                false,
                CURRENT.getFieldBoolean("isInt1").not(),
                "{'$not': {'$getField': {'input': '$$CURRENT', 'field': 'isInt1'}}}");
    }

    @Test
    public void arrayExpressionsTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#array-expression-operators
        // (Incomplete)

        // literals
        ArrayExpression<IntegerExpression> array123 = Expressions.ofIntegerArray(1, 2, 3);

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/filter/
        assertExpression(
                Stream.of(1, 2, 3)
                        .filter(v -> v == 2).collect(Collectors.toList()),
                array123.filter(v -> v.eq(Expressions.ofInteger(2))),
                // MQL:
                "{'$filter': {'input': [1, 2, 3], 'cond': {'$eq': ['$$this', 2]}}}");

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/map/
        assertExpression(
                Stream.of(1, 2, 3)
                        .map(v -> v * 2).collect(Collectors.toList()),
                array123.map(v -> v.multiply(2)),
                // MQL:
                "{'$map': {'input': [1, 2, 3], 'in': {'$multiply': ['$$this', 2]}}}");

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/reduce/
        assertExpression(
                Stream.of(1, 2, 3)
                        .reduce(1, (a, b) -> a * b),
                array123.reduce(Expressions.ofInteger(1), (a, b) -> a.multiply(b)),
                // TODO: implicit monoid?
                // MQL:
                "{'$reduce': {'input': [1, 2, 3], 'initialValue': 1, 'in': {'$multiply': ['$$this', '$$value']}}}");

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/setUnion/ (40) aka distinct?
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/size/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/arrayElemAt/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/in/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/concatArrays/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/slice/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/first/

        // fields:
        getCollectionHelper().insertDocuments("[{ 'arrObj': [ {v: true}, {v: false} ] }]");
        assertExpression(
                BsonArray.parse("[{'v': true}, {'v': false}]"),
                CURRENT.getFieldDocumentArray("arrObj"),
                "{'$getField': {'input': '$$CURRENT', 'field': 'arrObj'}}");

        assertExpression(
                BsonArray.parse("[true, false]"),
                CURRENT.getFieldDocumentArray("arrObj")
                        .map(d -> d.getFieldBoolean("v")),
                " {'$map': {'input': {'$getField': {"
                        + "'input': '$$CURRENT', 'field': 'arrObj'}}, "
                        + "'in': {'$getField': {'input': '$$this', 'field': 'v'}}}}");
    }

    @Test
    public void documentExpressionsTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#object-expression-operators
        // (Complete as of 6.0)

        // literals
        DocumentExpression a1 = Expressions.ofDocument(BsonDocument.parse("{a: 1}"));
        DocumentExpression ax1ay2 = Expressions.ofDocument(BsonDocument.parse("{a: {x: 1, y: 2}}"));
        DocumentExpression nullDoc = Expressions.ofDocument(null);
        DocumentExpression emptyDoc = Expressions.ofDocument(BsonDocument.parse("{}"));
        DocumentExpression literalDoc = Expressions.ofDocument(BsonDocument.parse("{lit: {'$not': true} }"));

        assertExpression(BsonDocument.parse("{'a': 1}"), a1, "{'$literal': {'a': 1}}");
        assertExpression(BsonDocument.parse("{'a': {'x': 1, 'y': 2}}"), ax1ay2, "{'$literal': {'a': {'x': 1, 'y': 2}}}");
        assertExpression(null, nullDoc, "null");
        assertExpression(BsonDocument.parse("{}"), emptyDoc, "{'$literal': {}}");
        assertExpression(BsonDocument.parse("{'lit': {'$not': true}}"), literalDoc, "{'$literal': {'lit': {'$not': true}}}");
        assertTrue(a1 instanceof DocumentExpression);

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/getField/ (100)
        assertExpression(1, a1.getFieldInteger("a"), "{'$getField': {'input': {'$literal': {'a': 1}}, 'field': 'a'}}");
        assertExpression(2, a1.getFieldInteger("a").multiply(2), "{'$multiply': [{'$getField': {'input': {'$literal': {'a': 1}}, 'field': 'a'}}, 2]}");

        // missing fields
        assertExpression(
                BsonDocument.parse("{'a': 1}"),
                a1.setField("z", a1.getFieldBoolean("missing")));
        assertExpression(
                BsonDocument.parse("{'a': 1}"),
                a1.setField("z", a1.getFieldDocument("missing").getFieldDocument("also_missing")));
        assertExpression(
                BsonDocument.parse("{'a': 1, 'z': ''}"),
                a1.setField("z", a1.getFieldString("missing").toLower()));
        /*
        The behaviour of missing fields appears to be as follows, and equivalent to $$REMOVE:
        propagates -- getField, cond branches...
        false -- not, or, cond check...
        0     -- sum...
        ""    -- toLower...
        null  -- multiply, add, subtract, year, filter, reduce, map, result within map...
        */

        // different types
        String getFieldMql = "{'$getField': {'input': {'$literal': {'a': 1}}, 'field': 'a'}}";
        assertExpression(1, a1.getFieldBoolean("a"), getFieldMql);
        assertExpression(1, a1.getFieldNumber("a"), getFieldMql);
        assertExpression(1, a1.getFieldInteger("a"), getFieldMql);
        assertExpression(1, a1.getFieldString("a"), getFieldMql);
        assertExpression(1, a1.getFieldDate("a"), getFieldMql);
        assertExpression(1, a1.getFieldArray("a"), getFieldMql);
        assertExpression(1, a1.getFieldDocument("a"), getFieldMql);
        assertExpression(1, a1.getFieldDocumentArray("a"), getFieldMql);
        assertTrue(a1.getFieldBoolean("a") instanceof BooleanExpression);
        assertTrue(a1.getFieldNumber("a") instanceof NumberExpression);
        assertTrue(a1.getFieldInteger("a") instanceof IntegerExpression);
        assertTrue(a1.getFieldInteger("a") instanceof NumberExpression);
        assertTrue(a1.getFieldString("a") instanceof StringExpression);
        assertTrue(a1.getFieldDate("a") instanceof DateExpression);
        assertTrue(a1.getFieldArray("a") instanceof ArrayExpression);
        assertTrue(a1.getFieldDocument("a") instanceof DocumentExpression);
        assertTrue(a1.getFieldDocumentArray("a") instanceof ArrayExpression);

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/setField/
        // "setField" is "withField": nothing is modified, and a new copy is produced.
        // Placing a field based on a literal:
        assertExpression(
                BsonDocument.parse("{a: 1, r: 2}"), // map.put("r", 2)
                a1.setField("r", Expressions.ofInteger(2)),
                // MQL:
                "{'$setField': {'field': 'r', 'input': {'$literal': {'a': 1}}, 'value': 2}}");

        // Placing a null value:
        assertExpression(
                BsonDocument.parse("{a: 1, r: null}"), // map.put("r", null)
                a1.setField("r", Expressions.ofNull()),
                // MQL:
                "{'$setField': {'field': 'r', 'input': {'$literal': {'a': 1}}, 'value': null}}");

        // Removing a field: // TODO note: introduces a new method
        assertExpression(
                BsonDocument.parse("{}"), // map.remove("a")
                a1.removeField("a"),
                // MQL:
                "{'$setField': {'field': 'a', 'input': {'$literal': {'a': 1}}, 'value': '$$REMOVE'}}");

        // Replacing a field based on its prior value:
        assertExpression(
                BsonDocument.parse("{a: 3}"), // map.put("a", map.get("a") * 3)
                a1.setField("a", a1.getFieldInteger("a").multiply(3)),
                // MQL:
                "{'$setField': {'field': 'a', 'input': {'$literal': {'a': 1}}, 'value': "
                        + "{'$multiply': [{'$getField': {'input': {'$literal': {'a': 1}}, 'field': 'a'}}, 3]}}}");

        // Placing a field based on a nested object:
        assertExpression(
                BsonDocument.parse("{'a': {'x': 1, 'y': 2}, r: 10}"),
                ax1ay2.setField("r", ax1ay2.getFieldDocument("a").getFieldInteger("x").multiply(10)),
                // MQL:
                "{'$setField': {'field': 'r', 'input': {'$literal': {'a': {'x': 1, 'y': 2}}}, "
                        + "'value': {'$multiply': ["
                        + "  {'$getField': {'input': {'$getField': {'input': {'$literal': {'a': {'x': 1, 'y': 2}}}, "
                        + "  'field': 'a'}}, 'field': 'x'}}, 10]}}}");

        // Replacing a nested object requires two setFields, as expected:
        assertExpression(
                // "with" syntax: [ { a:{x:1,y:2} } ].map(d -> d.with("a", d.a.with("y", d.a.y.multiply(11))))
                BsonDocument.parse("{'a': {'x': 1, 'y': 22}}"),
                ax1ay2.setField("a", ax1ay2.getFieldDocument("a")
                        .setField("y", ax1ay2.getFieldDocument("a").getFieldInteger("y").multiply(11))),
                "{'$setField': {'field': 'a', 'input': {'$literal': {'a': {'x': 1, 'y': 2}}}, "
                        + "'value': {'$setField': {'field': 'y', 'input': {'$getField': "
                        + "{'input': {'$literal': {'a': {'x': 1, 'y': 2}}}, 'field': 'a'}}, "
                        + "'value': {'$multiply': [{'$getField': {'input': {'$getField': "
                        + "{'input': {'$literal': {'a': {'x': 1, 'y': 2}}}, 'field': 'a'}}, "
                        + "'field': 'y'}}, 11]}}}}}");

        // The expression directly above, condensed using a helper class:
        ExampleA d = new ExampleA();
        d.setA(d.a.setY(d.a.y.multiply(2)));
        // TODO: assertion

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/arrayToObject/ (48)
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/objectToArray/ (23)
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/mergeObjects/

    }

    @Test
    public void specialExpressionsTest() {
        // TODO work in progress

        // special values
        assertExpression(null, ofNull(), "null");
        // the "missing" value is obtained via getField
        // the "$$REMOVE" value is intentionally not exposed. It is used internally
        // the "undefined" value is deprecated

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/literal/
        // $literal is intentionally not exposed. It is used internally.

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/let/
        IntegerExpression one = Expressions.ofInteger(1);
        Function<IntegerExpression, IntegerExpression> decrement = (e) -> e.subtract(one);

        // Simple arithmetic:
        assertExpression(
                (1 + 1) - 1,
                one.add(one).subtract(one));

        // Applying the subtraction via a lambda forces a discouraged nested syntax:
        assertExpression(
                (1 + 1) - 1,
                decrement.apply(one.add(one)));

        // Chaining into an inline lambda:
        assertExpression(
                (1 + 1) - 1,
                one.add(one).dot((IntegerExpression e) -> e.subtract(one)));

        // Chaining into a lambda variable (or method) solves the nested syntax problem:
        assertExpression(
                ((1 + 1) - 1) - 1, // = 0
                one.add(one).dot(decrement).dot(decrement),
                "{'$subtract': [{'$subtract': [{'$add': [1, 1]}, 1]}, 1]}");

        // To move the parenthesis, we use "let":
        assertExpression(
                (1 + 1) - (1 - 1), // = 2
                one.add(one).let(
                        one.dot(decrement),
                        (IntegerExpression two, IntegerExpression zero) -> two.subtract(zero)),
                "{'$let': {'vars': {'var1': {'$subtract': [1, 1]}}, "
                        + "'in': {'$subtract': [{'$add': [1, 1]}, '$$var1']}}}");
        /*
        The "dot" function is a zero-variable "let".

        This above "let" is not equivalent to the nested functional let. In both
        the above and the nested syntax, the variable "zero" cannot be used when
        defining the variable. But in the above syntax, "zero" cannot be used in
        the initiating expression, "one.add(one)". Previously, the initiating
        expression was inside the parenthesis, now it is outside.

        If "let" needs to be used over that original expression then it can move
        up/left without issue. The generated MQL is identical:
        */
        assertExpression(
                (1 + 1) - (1 - 1), // = 2
                one.let(
                        one.dot(decrement),
                        (IntegerExpression e, IntegerExpression zero) -> e.add(one).subtract(zero)),
                "{'$let': {'vars': {'var1': {'$subtract': [1, 1]}}, " //  ^^^^^^^^^ moved "down/right"
                        + "'in': {'$subtract': [{'$add': [1, 1]}, '$$var1']}}}");

        // TODO naming: dot, apply, map, chain?
        // TODO cf: setField("three", three.dot(triple))
        // TODO let must have different variable names. Perhaps a post-processor.

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/switch/
        /*
        The 'branches' field of switch cannot be an arbitrary expression. Even
        single branches cannot be an arbitrary expressions, though the values
        of "case" and "then" may be arbitrary expressions.

        This is good, because it has correct control flow, but it means that we
        cannot easily construct branches out of existing constructs, like arrays
        and documents.

        JSON/MQL switch does not eq-match. The switched item must be repeated.

        If no default is present, switch emits an error.
        */

        BooleanExpression tru = Expressions.ofBoolean(true);
        BooleanExpression fal = Expressions.ofBoolean(false);

        StringExpression thenA = Expressions.ofString("A");
        StringExpression thenB = Expressions.ofString("B");
        StringExpression thenC = Expressions.ofString("C");
        StringExpression elseD = Expressions.ofString("default");

        // Expressed, inconveniently, as a cond-sequence:
        assertExpression(
                "C",
                fal.cond(thenA,
                        fal.cond(thenB,
                                tru.cond(thenC, elseD)))
        );

        // A switch is similar to an Optional chain.
        assertExpression(
                "C",
                ofNull()
                        .ifNull(tru.eq(fal).cond(thenA, ofNull()))
                        .ifNull(tru.eq(fal).cond(thenB, ofNull()))
                        .ifNull(tru.eq(tru).cond(thenC, ofNull()))
                        .ifNull(elseD)
        );
        // This would not work, since individual branches can produce null, and
        // there is no other suitable "missing" or "empty" value.

        // The above suggests something like the following, which requires a
        // separate, non-Expression class, which is produced via the lambda:
        BooleanExpression value = Expressions.ofBoolean(true);
        value.switchMap((e, init) -> init
                .caseEq(fal, thenA)
                .caseEq(fal, thenB)
                .caseEq(tru, thenC)
                .defaults(elseD)
        );

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/meta/
    }


    @Test
    public void comparisonExpressionsTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#comparison-expression-operators
        // (Complete as of 6.0)
        // Comparison expressions are on the generic Expression class.

        BooleanExpression tru = Expressions.ofBoolean(true);
        StringExpression str = Expressions.ofString("string");
        Expression nul = Expressions.ofNull();

        // equality:
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/eq/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/ne/

        // comparison:
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/gt/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/gte/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/lt/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/lte/


        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/cmp/ (89)
        // TODO implement cmp as a three-branch cond?

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/ifNull/
        // This is similar to orElse, but treats nulls as empty Optionals
        // TODO the last position is auto-filled with null
        // TODO this diverges from the MQL
        // TODO just as with 1.add(2).add(3), multi-param ifNull should be chained: a.ifNull(b).ifNull(c)

        assertExpression(
                true,
                nul.ifNull(tru).ifNull(str),
                // MQL
                "{'$ifNull': [{'$ifNull': [null, true, null]}, 'string', null]}");
        assertExpression(
                "string",
                nul.ifNull(nul).ifNull(str),
                // MQL
                "{'$ifNull': [{'$ifNull': [null, null, null]}, 'string', null]}");
    }





    @Test
    public void arithmeticExpressionsTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#arithmetic-expression-operators

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/add/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/sum/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/multiply/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/subtract/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/divide/

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/max/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/min/ (63)
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/round/

        // TODO helper for bound constrain via max/min?

//        assertEval(
//                doc.num + 10,
//                fieldNum("num").add(of(10)));
//        assertEval(
//                doc.num + 1.5 + 100,
//                fieldNum("num").add(1.5).add(of(100)));


    }

    @Test
    public void stringExpressionsTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#string-expression-operators

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/concat/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateFromString/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/substr/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toLower/

    }

    @Test
    public void dateExpressionsTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#date-expression-operators

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateToString/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/year/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/month/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfMonth/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/hour/

    }

    @Test
    public void typeExpressionsTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#type-expression-operators

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/convert/

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/isArray/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/isNumber/

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toString/

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toDate/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toObjectId/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toInt/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/type/

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toBool/ (83)

    }

    @BeforeEach
    public void setUp() {
        getCollectionHelper().drop();
    }

    @AfterEach
    public void tearDown() {
        getCollectionHelper().drop();
    }

    private void assertExpression(final Object result, final Expression exp) {
        assertExpression(result, exp, null);
    }
    private void assertExpression(final Object result, final Expression exp, final String string) {
        assertEval(result, exp);

        // TODO start delete
        String revisedString = "".equals(string) ?  "'invalid string'" : string;
        if (string == null) {
            return;
        }
        // TODO end delete

        BsonValue expressionValue = ((MqlExpression) exp).toBsonValue(fromProviders(new BsonValueCodecProvider()));
        BsonValue bsonValue = new BsonDocumentFragmentCodec().readValue(
                new JsonReader(revisedString),
                DecoderContext.builder().build());
        assertEquals(bsonValue, expressionValue, expressionValue.toString().replace("\"", "'"));
    }

    private void assertEval(final Object expected, final Expression toEvaluate) {
        Bson addFieldsStage = addFields(new Field<>("val", toEvaluate));
        List<Bson> stages = new ArrayList<>();
        stages.add(addFieldsStage);
        List<BsonDocument> results;
        if (getCollectionHelper().count() == 0) {
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

    // TODO: non-functional:
    static class ExampleA extends MqlExpression<Expression> {
        private ExampleXY a = new ExampleXY(this.getFieldDocument("a"));
        protected ExampleA() {
            this(cr -> new BsonDocument());
        }
        protected ExampleA(final Function<CodecRegistry, BsonValue> fn) {
            super(fn);
        }
        public ExampleA setA(final ExampleXY a) {
            return this.setField("a", a);
        }
        protected <Q extends Expression> Q newMqlExpression(final Function<CodecRegistry, BsonValue> ast) {
            // TODO
            return (Q) new ExampleA(ast);
        }
    }

    static class ExampleXY extends MqlExpression<Expression> {
        private IntegerExpression x = this.getFieldInteger("x");
        private IntegerExpression y = this.getFieldInteger("y");
        protected ExampleXY(final Function<CodecRegistry, BsonValue> fn) {
            super(fn);
        }
        protected ExampleXY(final DocumentExpression a) {
            super((cr) -> null);
        }
        public ExampleXY setY(final IntegerExpression multiply) {
            return this;
        }
    }
}

