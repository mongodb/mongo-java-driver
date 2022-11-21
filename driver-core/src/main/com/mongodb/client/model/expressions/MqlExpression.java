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

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Collections;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static com.mongodb.client.model.expressions.Expressions.of;
import static com.mongodb.client.model.expressions.Expressions.ofStringArray;

final class MqlExpression<T extends Expression>
        implements Expression, BooleanExpression, IntegerExpression, NumberExpression,
        StringExpression, DateExpression, DocumentExpression, ArrayExpression<T> {

    private final Function<CodecRegistry, AstPlaceholder> fn;

    MqlExpression(final Function<CodecRegistry, AstPlaceholder> fn) {
        this.fn = fn;
    }

    /**
     * Exposes the evaluated BsonValue so that expressions may be used in
     * aggregations. Non-public, as it is intended to be used only by the
     * {@link MqlExpressionCodec}.
     */
    BsonValue toBsonValue(final CodecRegistry codecRegistry) {
        return fn.apply(codecRegistry).bsonValue;
    }

    private AstPlaceholder astDoc(final String name, final BsonDocument value) {
        return new AstPlaceholder(new BsonDocument(name, value));
    }

    static final class AstPlaceholder {
        private final BsonValue bsonValue;

        AstPlaceholder(final BsonValue bsonValue) {
            this.bsonValue = bsonValue;
        }
    }

    private Function<CodecRegistry, AstPlaceholder> ast(final String name) {
        return (cr) -> new AstPlaceholder(new BsonDocument(name, this.toBsonValue(cr)));
    }

    // in cases where we must wrap the first argument in an array
    private Function<CodecRegistry, AstPlaceholder> astWrapped(final String name) {
        return (cr) -> new AstPlaceholder(new BsonDocument(name,
                new BsonArray(Collections.singletonList(this.toBsonValue(cr)))));
    }

    private Function<CodecRegistry, AstPlaceholder> ast(final String name, final Expression param1) {
        return (cr) -> {
            BsonArray value = new BsonArray();
            value.add(this.toBsonValue(cr));
            value.add(extractBsonValue(cr, param1));
            return new AstPlaceholder(new BsonDocument(name, value));
        };
    }

    private Function<CodecRegistry, AstPlaceholder> ast(final String name, final Expression param1, final Expression param2) {
        return (cr) -> {
            BsonArray value = new BsonArray();
            value.add(this.toBsonValue(cr));
            value.add(extractBsonValue(cr, param1));
            value.add(extractBsonValue(cr, param2));
            return new AstPlaceholder(new BsonDocument(name, value));
        };
    }

    /**
     * Takes an expression and converts it to a BsonValue. MqlExpression will be
     * the only implementation of Expression and all subclasses, so this will
     * not mis-cast an expression as anything else.
     */
    private static BsonValue extractBsonValue(final CodecRegistry cr, final Expression expression) {
        return ((MqlExpression<?>) expression).toBsonValue(cr);
    }

    /**
     * Converts an MqlExpression to any subtype of Expression. Users must not
     * extend Expression or its subtypes, so MqlExpression will implement any R.
     */
    @SuppressWarnings("unchecked")
    <R extends Expression> R assertImplementsAllExpressions() {
        return (R) this;
    }

    private static <R extends Expression> R newMqlExpression(final Function<CodecRegistry, AstPlaceholder> ast) {
        return new MqlExpression<>(ast).assertImplementsAllExpressions();
    }

    private <R extends Expression> R variable(final String variable) {
        return newMqlExpression((cr) -> new AstPlaceholder(new BsonString(variable)));
    }

    /** @see BooleanExpression */

    @Override
    public BooleanExpression not() {
        return new MqlExpression<>(ast("$not"));
    }

    @Override
    public BooleanExpression or(final BooleanExpression or) {
        return new MqlExpression<>(ast("$or", or));
    }

    @Override
    public BooleanExpression and(final BooleanExpression and) {
        return new MqlExpression<>(ast("$and", and));
    }

    @Override
    public <R extends Expression> R cond(final R left, final R right) {
        return newMqlExpression(ast("$cond", left, right));
    }


    /** @see Expression */

    @Override
    public BooleanExpression eq(final Expression eq) {
        return new MqlExpression<>(ast("$eq", eq));
    }

    @Override
    public BooleanExpression ne(final Expression ne) {
        return new MqlExpression<>(ast("$ne", ne));
    }

    @Override
    public BooleanExpression gt(final Expression gt) {
        return new MqlExpression<>(ast("$gt", gt));
    }

    @Override
    public BooleanExpression gte(final Expression gte) {
        return new MqlExpression<>(ast("$gte", gte));
    }

    @Override
    public BooleanExpression lt(final Expression lt) {
        return new MqlExpression<>(ast("$lt", lt));
    }

    @Override
    public BooleanExpression lte(final Expression lte) {
        return new MqlExpression<>(ast("$lte", lte));
    }

    public BooleanExpression isBoolean() {
        return new MqlExpression<>(ast("$type")).eq(of("bool"));
    }

    @Override
    public BooleanExpression isBooleanOr(final BooleanExpression or) {
        return this.isBoolean().cond(this, or);
    }

    public BooleanExpression isNumber() {
        return new MqlExpression<>(astWrapped("$isNumber"));
    }

    @Override
    public NumberExpression isNumberOr(final NumberExpression or) {
        return this.isNumber().cond(this, or);
    }

    public BooleanExpression isString() {
        return new MqlExpression<>(ast("$type")).eq(of("string"));
    }

    @Override
    public StringExpression isStringOr(final StringExpression or) {
        return this.isString().cond(this, or);
    }

    public BooleanExpression isDate() {
        return ofStringArray("date", "timestamp").contains(new MqlExpression<>(ast("$type")));
    }

    @Override
    public DateExpression isDateOr(final DateExpression or) {
        return this.isDate().cond(this, or);
    }

    public BooleanExpression isArray() {
        return new MqlExpression<>(astWrapped("$isArray"));
    }

    @SuppressWarnings("unchecked") // TODO
    @Override
    public ArrayExpression<Expression> isArrayOr(final ArrayExpression<? extends Expression> or) {
        // TODO it seems that ArrEx<T> does not make sense here
        return (ArrayExpression<Expression>) this.isArray().cond(this.assertImplementsAllExpressions(), or);
    }

    public BooleanExpression isDocument() {
        return new MqlExpression<>(ast("$type")).eq(of("object"));
    }

    @Override
    public <R extends DocumentExpression> R isDocumentOr(final R or) {
        return this.isDocument().cond(this.assertImplementsAllExpressions(), or);
    }

    @Override
    public StringExpression asString() {
        return new MqlExpression<>(astWrapped("$toString"));
    }

    @Override
    public IntegerExpression parseInteger() {
        return new MqlExpression<>(ast("$toLong")); // TODO
    }

    /** @see ArrayExpression */

    @Override
    public <R extends Expression> ArrayExpression<R> map(final Function<? super T, ? extends R> in) {
        T varThis = variable("$$this");
        return new MqlExpression<>((cr) -> astDoc("$map", new BsonDocument()
                .append("input", this.toBsonValue(cr))
                .append("in", extractBsonValue(cr, in.apply(varThis)))));
    }

    @Override
    public ArrayExpression<T> filter(final Function<? super T, ? extends BooleanExpression> cond) {
        T varThis = variable("$$this");
        return new MqlExpression<>((cr) -> astDoc("$filter", new BsonDocument()
                .append("input", this.toBsonValue(cr))
                .append("cond", extractBsonValue(cr, cond.apply(varThis)))));
    }

    @Override
    public T reduce(final T initialValue, final BinaryOperator<T> in) {
        T varThis = variable("$$this");
        T varValue = variable("$$value");
        return newMqlExpression((cr) -> astDoc("$reduce", new BsonDocument()
                .append("input", this.toBsonValue(cr))
                .append("initialValue", extractBsonValue(cr, initialValue))
                .append("in", extractBsonValue(cr, in.apply(varValue, varThis)))));
    }

    @Override
    public IntegerExpression size() {
        return new MqlExpression<>(astWrapped("$size"));
    }

    @Override
    public T elementAt(final IntegerExpression at) {
        return new MqlExpression<>(ast("$arrayElemAt", at))
                .assertImplementsAllExpressions();
    }

    @Override
    public T first() {
        return new MqlExpression<>(astWrapped("$first"))
                .assertImplementsAllExpressions();
    }

    @Override
    public T last() {
        return new MqlExpression<>(astWrapped("$last"))
                .assertImplementsAllExpressions();
    }

    @Override
    public BooleanExpression contains(final T item) {
        String name = "$in";
        return new MqlExpression<>((cr) -> {
            BsonArray value = new BsonArray();
            value.add(extractBsonValue(cr, item));
            value.add(this.toBsonValue(cr));
            return new AstPlaceholder(new BsonDocument(name, value));
        }).assertImplementsAllExpressions();
    }

    @Override
    public ArrayExpression<T> concat(final ArrayExpression<T> array) {
        return new MqlExpression<>(ast("$concatArrays", array))
                .assertImplementsAllExpressions();
    }

    @Override
    public ArrayExpression<T> slice(final IntegerExpression start, final IntegerExpression length) {
        return new MqlExpression<>(ast("$slice", start, length))
                .assertImplementsAllExpressions();
    }

    @Override
    public ArrayExpression<T> union(final ArrayExpression<T> set) {
        return new MqlExpression<>(ast("$setUnion", set))
                .assertImplementsAllExpressions();
    }

    @Override
    public ArrayExpression<T> distinct() {
        return new MqlExpression<>(astWrapped("$setUnion"));
    }


    /** @see IntegerExpression
     *  @see NumberExpression */

    @Override
    public IntegerExpression multiply(final NumberExpression n) {
        return newMqlExpression(ast("$multiply", n));
    }

    @Override
    public NumberExpression add(final NumberExpression n) {
        return new MqlExpression<>(ast("$add", n));
    }

    @Override
    public NumberExpression divide(final NumberExpression n) {
        return new MqlExpression<>(ast("$divide", n));
    }

    @Override
    public NumberExpression max(final NumberExpression n) {
        return new MqlExpression<>(ast("$max", n));
    }

    @Override
    public NumberExpression min(final NumberExpression n) {
        return new MqlExpression<>(ast("$min", n));
    }

    @Override
    public IntegerExpression round() {
        return new MqlExpression<>(ast("$round"));
    }

    @Override
    public NumberExpression round(final IntegerExpression place) {
        return new MqlExpression<>(ast("$round", place));
    }

    @Override
    public IntegerExpression multiply(final IntegerExpression i) {
        return new MqlExpression<>(ast("$multiply", i));
    }

    @Override
    public IntegerExpression abs() {
        return newMqlExpression(ast("$abs"));
    }

    @Override
    public DateExpression msToDate() {
        return newMqlExpression(ast("$toDate"));
    }

    @Override
    public NumberExpression subtract(final NumberExpression n) {
        return new MqlExpression<>(ast("$subtract", n));
    }

    @Override
    public IntegerExpression add(final IntegerExpression i) {
        return new MqlExpression<>(ast("$add", i));
    }

    @Override
    public IntegerExpression subtract(final IntegerExpression i) {
        return new MqlExpression<>(ast("$subtract", i));
    }

    @Override
    public IntegerExpression max(final IntegerExpression i) {
        return new MqlExpression<>(ast("$max", i));
    }

    @Override
    public IntegerExpression min(final IntegerExpression i) {
        return new MqlExpression<>(ast("$min", i));
    }

    /** @see DateExpression */

    private MqlExpression<Expression> usingTimezone(final String name, final StringExpression timezone) {
        return new MqlExpression<>((cr) -> astDoc(name, new BsonDocument()
                .append("date", this.toBsonValue(cr))
                .append("timezone", extractBsonValue(cr, timezone))));
    }

    @Override
    public IntegerExpression year(final StringExpression timezone) {
        return usingTimezone("$year", timezone);
    }

    @Override
    public IntegerExpression month(final StringExpression timezone) {
        return usingTimezone("$month", timezone);
    }

    @Override
    public IntegerExpression dayOfMonth(final StringExpression timezone) {
        return usingTimezone("$dayOfMonth", timezone);
    }

    @Override
    public IntegerExpression dayOfWeek(final StringExpression timezone) {
        return usingTimezone("$dayOfWeek", timezone);
    }

    @Override
    public IntegerExpression dayOfYear(final StringExpression timezone) {
        return usingTimezone("$dayOfYear", timezone);
    }

    @Override
    public IntegerExpression hour(final StringExpression timezone) {
        return usingTimezone("$hour", timezone);
    }

    @Override
    public IntegerExpression minute(final StringExpression timezone) {
        return usingTimezone("$minute", timezone);
    }

    @Override
    public IntegerExpression second(final StringExpression timezone) {
        return usingTimezone("$second", timezone);
    }

    @Override
    public IntegerExpression week(final StringExpression timezone) {
        return usingTimezone("$week", timezone);
    }

    @Override
    public IntegerExpression millisecond(final StringExpression timezone) {
        return usingTimezone("$millisecond", timezone);
    }

    @Override
    public StringExpression asString(final StringExpression timezone, final StringExpression format) {
        return newMqlExpression((cr) -> astDoc("$dateToString", new BsonDocument()
                .append("date", this.toBsonValue(cr))
                .append("format", extractBsonValue(cr, format))
                .append("timezone", extractBsonValue(cr, timezone))));
    }

    @Override
    public DateExpression parseDate(final StringExpression format, final StringExpression timezone) {
        return newMqlExpression((cr) -> astDoc("$dateFromString", new BsonDocument()
                .append("dateString", this.toBsonValue(cr))
                .append("format", extractBsonValue(cr, format))
                .append("timezone", extractBsonValue(cr, timezone))));
    }

    @Override
    public DateExpression parseDate() {
        return newMqlExpression((cr) -> astDoc("$dateFromString", new BsonDocument()
                .append("dateString", this.toBsonValue(cr))));
    }

    /** @see StringExpression */

    @Override
    public StringExpression toLower() {
        return new MqlExpression<>(ast("$toLower"));
    }

    @Override
    public StringExpression toUpper() {
        return new MqlExpression<>(ast("$toUpper"));
    }

    @Override
    public StringExpression concat(final StringExpression concat) {
        return new MqlExpression<>(ast("$concat", concat));
    }

    @Override
    public IntegerExpression strLen() {
        return new MqlExpression<>(ast("$strLenCP"));
    }

    @Override
    public IntegerExpression strLenBytes() {
        return new MqlExpression<>(ast("$strLenBytes"));
    }

    @Override
    public StringExpression substr(final IntegerExpression start, final IntegerExpression length) {
        return new MqlExpression<>(ast("$substrCP", start, length));
    }

    @Override
    public StringExpression substrBytes(final IntegerExpression start, final IntegerExpression length) {
        return new MqlExpression<>(ast("$substrBytes", start, length));
    }

    @Override
    public Expression parseObjectId() {
        return newMqlExpression(ast("$toObjectId"));
    }
}
