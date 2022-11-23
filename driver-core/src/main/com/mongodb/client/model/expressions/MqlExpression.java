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
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Collections;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static com.mongodb.client.model.expressions.Expressions.of;
import static com.mongodb.client.model.expressions.Expressions.ofNull;
import static com.mongodb.client.model.expressions.Expressions.ofStringArray;

final class MqlExpression<T extends Expression>
        implements Expression, BooleanExpression, IntegerExpression, NumberExpression,
        StringExpression, DateExpression, DocumentExpression, ArrayExpression<T>, MapExpression<T>, EntryExpression<T> {

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

    @Override
    public StringExpression getKey() {
        return new MqlExpression<>(getFieldInternal("k"));
    }

    @Override
    public T getValue() {
        return newMqlExpression(getFieldInternal("v"));
    }

    @Override
    public EntryExpression<T> setValue(final T value) {
        return setFieldInternal("v", value);
    }

    @Override
    public EntryExpression<T> setKey(final StringExpression key) {
        return setFieldInternal("k", key);
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
    static BsonValue extractBsonValue(final CodecRegistry cr, final Expression expression) {
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

    /** @see DocumentExpression */

    private Function<CodecRegistry, AstPlaceholder> getFieldInternal(final String fieldName) {
        return (cr) -> {
            BsonValue value = fieldName.startsWith("$")
                    ? new BsonDocument("$literal", new BsonString(fieldName))
                    : new BsonString(fieldName);
            return astDoc("$getField", new BsonDocument()
                    .append("input", this.fn.apply(cr).bsonValue)
                    .append("field", value));
        };
    }

    @Override
    public Expression getField(final String fieldName) {
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public BooleanExpression getBoolean(final String fieldName) {
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public BooleanExpression getBoolean(final String fieldName, final BooleanExpression other) {
        return getBoolean(fieldName).isBooleanOr(other);
    }

    @Override
    public NumberExpression getNumber(final String fieldName) {
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public NumberExpression getNumber(final String fieldName, final NumberExpression other) {
        return getNumber(fieldName).isNumberOr(other);
    }

    @Override
    public IntegerExpression getInteger(final String fieldName) {
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public IntegerExpression getInteger(final String fieldName, final IntegerExpression other) {
        return getInteger(fieldName).isIntegerOr(other);
    }

    @Override
    public StringExpression getString(final String fieldName) {
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public StringExpression getString(final String fieldName, final StringExpression other) {
        return getString(fieldName).isStringOr(other);
    }

    @Override
    public DateExpression getDate(final String fieldName) {
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public DateExpression getDate(final String fieldName, final DateExpression other) {
        return getDate(fieldName).isDateOr(other);
    }

    @Override
    public DocumentExpression getDocument(final String fieldName) {
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public <R extends Expression> MapExpression<R> getMap(final String field) {
        return new MqlExpression<>(getFieldInternal(field));
    }

    @Override
    public <R extends Expression> MapExpression<R> getMap(final String field, final MapExpression<? extends R> other) {
        return getMap(field).isMapOr(other);
    }

    @Override
    public DocumentExpression getDocument(final String fieldName, final DocumentExpression other) {
        return getDocument(fieldName).isDocumentOr(other);
    }

    @Override
    public <R extends Expression> ArrayExpression<R> getArray(final String fieldName) {
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public <R extends Expression> ArrayExpression<R> getArray(final String fieldName, final ArrayExpression<? extends R> other) {
        return getArray(fieldName).isArrayOr(other);
    }

    @Override
    public DocumentExpression merge(final DocumentExpression other) {
        return new MqlExpression<>(ast("$mergeObjects", other));
    }

    @Override
    public DocumentExpression setField(final String fieldName, final Expression exp) {
        return setFieldInternal(fieldName, exp);
    }

    private MqlExpression<T> setFieldInternal(final String fieldName, final Expression exp) {
        return newMqlExpression((cr) -> astDoc("$setField", new BsonDocument()
                .append("field", new BsonString(fieldName))
                .append("input", this.toBsonValue(cr))
                .append("value", extractBsonValue(cr, exp))));
    }

    @Override
    public DocumentExpression unsetField(final String fieldName) {
        return newMqlExpression((cr) -> astDoc("$unsetField", new BsonDocument()
                .append("field", new BsonString(fieldName))
                .append("input", this.toBsonValue(cr))));
    }

    /** @see Expression */

    @Override
    public <Q extends Expression, R extends Expression> R apply(final Function<Q, R> f) {
        return f.apply(this.assertImplementsAllExpressions());
    }

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
        return new MqlExpression<>(astWrapped("$type")).eq(of("bool"));
    }

    @Override
    public BooleanExpression isBooleanOr(final BooleanExpression other) {
        return this.isBoolean().cond(this, other);
    }

    public BooleanExpression isNumber() {
        return new MqlExpression<>(astWrapped("$isNumber"));
    }

    @Override
    public NumberExpression isNumberOr(final NumberExpression other) {
        return this.isNumber().cond(this, other);
    }

    public BooleanExpression isInteger() {
        return this.isNumber().cond(this.eq(this.round()), of(false));
    }

    @Override
    public IntegerExpression isIntegerOr(final IntegerExpression other) {
        return this.isInteger().cond(this, other);
    }

    public BooleanExpression isString() {
        return new MqlExpression<>(astWrapped("$type")).eq(of("string"));
    }

    @Override
    public StringExpression isStringOr(final StringExpression other) {
        return this.isString().cond(this, other);
    }

    public BooleanExpression isDate() {
        return ofStringArray("date").contains(new MqlExpression<>(astWrapped("$type")));
    }

    @Override
    public DateExpression isDateOr(final DateExpression other) {
        return this.isDate().cond(this, other);
    }

    public BooleanExpression isArray() {
        return new MqlExpression<>(astWrapped("$isArray"));
    }

    private Expression ifNull(final Expression ifNull) {
        return new MqlExpression<>(ast("$ifNull", ifNull, ofNull()))
                .assertImplementsAllExpressions();
    }

    /**
     * checks if array (but cannot check type)
     * user asserts array is of type R
     *
     * @param other
     * @return
     * @param <R>
     */
    @SuppressWarnings("unchecked")
    @Override
    public <R extends Expression> ArrayExpression<R> isArrayOr(final ArrayExpression<? extends R> other) {
        return (ArrayExpression<R>) this.isArray().cond(this.assertImplementsAllExpressions(), other);
    }

    private BooleanExpression isDocumentOrMap() {
        return new MqlExpression<>(ast("$type")).eq(of("object"));
    }

    @Override
    public <R extends DocumentExpression> R isDocumentOr(final R other) {
        return this.isDocumentOrMap().cond(this.assertImplementsAllExpressions(), other);
    }

    @Override
    public <R extends Expression> MapExpression<R> isMapOr(final MapExpression<? extends R> other) {
        MqlExpression<?> isMap = (MqlExpression<?>) this.isDocumentOrMap();
        return newMqlExpression(isMap.ast("$cond", this.assertImplementsAllExpressions(), other));
    }

    public BooleanExpression isNull() {
        return this.eq(ofNull());
    }

    @Override
    public StringExpression asString() {
        return new MqlExpression<>(astWrapped("$toString"));
    }

    private Function<CodecRegistry, AstPlaceholder> convertInternal(final String to, final Expression other) {
        return (cr) -> astDoc("$convert", new BsonDocument()
                .append("input", this.fn.apply(cr).bsonValue)
                .append("onError", extractBsonValue(cr, other))
                .append("to", new BsonString(to)));
    }

    @Override
    public <T0 extends Expression, R0 extends Expression> R0 switchMap(
            final Function<Branches, BranchesTerminal<T0, R0>> switchMap) {
        T0 value = this.assertImplementsAllExpressions();
        BranchesTerminal<T0, R0> construct = switchMap.apply(new Branches());
        return switchMapInternal(value, construct);
    }

    private <T0 extends Expression, R0 extends Expression> R0 switchMapInternal(
            final T0 value, final BranchesTerminal<T0, R0> construct) {
        return newMqlExpression((cr) -> {
            BsonArray branches = new BsonArray();
            for (Function<T0, SwitchCase<R0>> fn : construct.getBranches()) {
                SwitchCase<R0> result = fn.apply(value);
                branches.add(new BsonDocument()
                        .append("case", extractBsonValue(cr, result.getCaseValue()))
                        .append("then", extractBsonValue(cr, result.getThenValue())));
            }
            BsonDocument switchBson = new BsonDocument().append("branches", branches);
            if (construct.getDefaults() != null) {
                switchBson = switchBson.append("default", extractBsonValue(cr, construct.getDefaults().apply(value)));
            }
            return astDoc("$switch", switchBson);
        });
    }

    @Override
    public IntegerExpression parseInteger() {
        Expression asLong = new MqlExpression<>(ast("$toLong"));
        return new MqlExpression<>(convertInternal("int", asLong));
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

    public ArrayExpression<T> sort() {
        return new MqlExpression<>((cr) -> astDoc("$sortArray", new BsonDocument()
                .append("input", this.toBsonValue(cr))
                .append("sortBy", new BsonInt32(1))));
    }

    public T reduce(final T initialValue, final BinaryOperator<T> in) {
        T varThis = variable("$$this");
        T varValue = variable("$$value");
        return newMqlExpression((cr) -> astDoc("$reduce", new BsonDocument()
                .append("input", this.toBsonValue(cr))
                .append("initialValue", extractBsonValue(cr, initialValue))
                .append("in", extractBsonValue(cr, in.apply(varValue, varThis)))));
    }

    @Override
    public BooleanExpression any(final Function<? super T, BooleanExpression> predicate) {
        MqlExpression<BooleanExpression> array = (MqlExpression<BooleanExpression>) this.map(predicate);
        return array.reduce(of(false), (a, b) -> a.or(b));
    }

    @Override
    public BooleanExpression all(final Function<? super T, BooleanExpression> predicate) {
        MqlExpression<BooleanExpression> array = (MqlExpression<BooleanExpression>) this.map(predicate);
        return array.reduce(of(true), (a, b) -> a.and(b));
    }

    @SuppressWarnings("unchecked")
    @Override
    public NumberExpression sum(final Function<? super T, ? extends NumberExpression> mapper) {
        // no sum that returns IntegerExpression, both have same erasure
        MqlExpression<NumberExpression> array = (MqlExpression<NumberExpression>) this.map(mapper);
        return array.reduce(of(0), (a, b) -> a.add(b));
    }

    @SuppressWarnings("unchecked")
    @Override
    public NumberExpression multiply(final Function<? super T, ? extends NumberExpression> mapper) {
        MqlExpression<NumberExpression> array = (MqlExpression<NumberExpression>) this.map(mapper);
        return array.reduce(of(0), (NumberExpression a, NumberExpression b) -> a.multiply(b));
    }

    @Override
    public T max(final T other) {
        return this.size().eq(of(0)).cond(other, this.maxN(of(1)).first());
    }

    @Override
    public T min(final T other) {
        return this.size().eq(of(0)).cond(other, this.minN(of(1)).first());
    }

    @Override
    public ArrayExpression<T> maxN(final IntegerExpression n) {
        return newMqlExpression((CodecRegistry cr) -> astDoc("$maxN", new BsonDocument()
                .append("input", extractBsonValue(cr, this))
                .append("n", extractBsonValue(cr, n))));
    }

    @Override
    public ArrayExpression<T> minN(final IntegerExpression n) {
        return newMqlExpression((CodecRegistry cr) -> astDoc("$minN", new BsonDocument()
                .append("input", extractBsonValue(cr, this))
                .append("n", extractBsonValue(cr, n))));
    }

    @Override
    public StringExpression join(final Function<? super T, StringExpression> mapper) {
        MqlExpression<StringExpression> array = (MqlExpression<StringExpression>) this.map(mapper);
        return array.reduce(of(""), (a, b) -> a.concat(b));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R extends Expression> ArrayExpression<R> concat(final Function<? super T, ? extends ArrayExpression<? extends R>> mapper) {
        MqlExpression<ArrayExpression<R>> array = (MqlExpression<ArrayExpression<R>>) this.map(mapper);
        return array.reduce(Expressions.ofArray(), (a, b) -> a.concat(b));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R extends Expression> ArrayExpression<R> union(final Function<? super T, ? extends ArrayExpression<? extends R>> mapper) {
        MqlExpression<ArrayExpression<R>> array = (MqlExpression<ArrayExpression<R>>) this.map(mapper);
        return array.reduce(Expressions.ofArray(), (a, b) -> a.union(b));
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
    public ArrayExpression<T> concat(final ArrayExpression<? extends T> array) {
        return new MqlExpression<>(ast("$concatArrays", array))
                .assertImplementsAllExpressions();
    }

    @Override
    public ArrayExpression<T> slice(final IntegerExpression start, final IntegerExpression length) {
        return new MqlExpression<>(ast("$slice", start, length))
                .assertImplementsAllExpressions();
    }

    @Override
    public ArrayExpression<T> union(final ArrayExpression<? extends T> set) {
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
    public DateExpression millisecondsToDate() {
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
    public DateExpression parseDate(final StringExpression timezone, final StringExpression format) {
        return newMqlExpression((cr) -> astDoc("$dateFromString", new BsonDocument()
                .append("dateString", this.toBsonValue(cr))
                .append("format", extractBsonValue(cr, format))
                .append("timezone", extractBsonValue(cr, timezone))));
    }

    @Override
    public DateExpression parseDate(final StringExpression format) {
        return newMqlExpression((cr) -> astDoc("$dateFromString", new BsonDocument()
                .append("dateString", this.toBsonValue(cr))
                .append("format", extractBsonValue(cr, format))));
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
    public BooleanExpression has(final StringExpression key) {
        return get(key).ne(ofRem());
    }

    static <R extends Expression> R ofRem() {
        // $$REMOVE is intentionally not exposed to users
        return new MqlExpression<>((cr) -> new MqlExpression.AstPlaceholder(new BsonString("$$REMOVE")))
                .assertImplementsAllExpressions();
    }

    /** @see MapExpression
     * @see EntryExpression */

    @Override
    public T get(final StringExpression key) {
        return newMqlExpression((cr) -> astDoc("$getField", new BsonDocument()
                .append("input", this.fn.apply(cr).bsonValue)
                .append("field", extractBsonValue(cr, key))));
    }

    @SuppressWarnings("unchecked")
    @Override
    public T get(final StringExpression key, final T other) {
        return (T) ((MqlExpression<?>) get(key)).ifNull(other);
    }

    @Override
    public MapExpression<T> set(final StringExpression key, final T value) {
        return newMqlExpression((cr) -> astDoc("$setField", new BsonDocument()
                .append("field", extractBsonValue(cr, key))
                .append("input", this.toBsonValue(cr))
                .append("value", extractBsonValue(cr, value))));
    }

    @Override
    public MapExpression<T> unset(final StringExpression key) {
        return newMqlExpression((cr) -> astDoc("$unsetField", new BsonDocument()
                .append("field", extractBsonValue(cr, key))
                .append("input", this.toBsonValue(cr))));
    }

    @Override
    public MapExpression<T> merge(final MapExpression<? extends T> map) {
        return new MqlExpression<>(ast("$mergeObjects", map));
    }

    @Override
    public ArrayExpression<EntryExpression<T>> entrySet() {
        return newMqlExpression(ast("$objectToArray"));
    }

    @Override
    public <R extends Expression> MapExpression<R> asMap(
            final Function<? super T, ? extends EntryExpression<? extends R>> mapper) {
        @SuppressWarnings("unchecked")
        MqlExpression<EntryExpression<? extends R>> array = (MqlExpression<EntryExpression<? extends R>>) this.map(mapper);
        return newMqlExpression(array.astWrapped("$arrayToObject"));
    }

    @SuppressWarnings("unchecked")
    @Override
    public MapExpression<Expression> asMap() {
        return (MapExpression<Expression>) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R extends DocumentExpression> R asDocument() {
        return (R) this;
    }
}
