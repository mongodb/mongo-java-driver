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

import java.util.function.BinaryOperator;
import java.util.function.Function;

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

    final static class AstPlaceholder {
        private final BsonValue bsonValue;

        AstPlaceholder(final BsonValue bsonValue) {
            this.bsonValue = bsonValue;
        }
    }

    private Function<CodecRegistry, AstPlaceholder> ast(final String name) {
        return (cr) -> new AstPlaceholder(new BsonDocument(name, this.toBsonValue(cr)));
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
        return new MqlExpression<T>((cr) -> astDoc("$filter", new BsonDocument()
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
                .append("in", extractBsonValue(cr, in.apply(varThis, varValue)))));
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
}
