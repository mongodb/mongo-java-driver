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

import java.util.function.BiFunction;
import java.util.function.Function;

public class MqlExpression<T extends Expression>
        implements Expression, BooleanExpression, IntegerExpression, NumberExpression,
        StringExpression, DateExpression, DocumentExpression, ArrayExpression<T> {

    private final Function<CodecRegistry, BsonValue> fn;

    protected MqlExpression(final Function<CodecRegistry, BsonValue> fn) {
        this.fn = fn;
    }

    /**
     * Exposes the evaluated BsonValue so that expressions may be used in
     * aggregations. Non-public, as it is intended to be used only by the
     * {@link ExpressionCodec}.
     */
    BsonValue toBsonValue(final CodecRegistry codecRegistry) {
        return fn.apply(codecRegistry);
    }

    private Function<CodecRegistry, BsonValue> astDoc(final String name, final BsonDocument value) {
        return (cr) -> new BsonDocument(name, value);
    }

    private Function<CodecRegistry, BsonValue> ast(final String name) {
        return (cr) -> new BsonDocument(name, this.toBsonValue(cr));
    }

    private Function<CodecRegistry, BsonValue> ast(final String name, final Expression param1) {
        return (cr) -> {
            BsonArray value = new BsonArray();
            value.add(this.toBsonValue(cr));
            value.add(extractBsonValue(cr, param1));
            return new BsonDocument(name, value);
        };
    }

    private Function<CodecRegistry, BsonValue> ast(final String name, final Expression param1, final Expression param2) {
        return (cr) -> {
            BsonArray value = new BsonArray();
            value.add(this.toBsonValue(cr));
            value.add(extractBsonValue(cr, param1));
            value.add(extractBsonValue(cr, param2));
            return new BsonDocument(name, value);
        };
    }

    /**
     * Takes an expression and converts it to a BsonValue. MqlExpression will be
     * the only implementation of Expression and all subclasses, so this will
     * not mis-cast an expression as anything else.
     */
    @SuppressWarnings("rawtypes")
    protected BsonValue extractBsonValue(final CodecRegistry cr, final Expression expression) {
        return ((MqlExpression) expression).toBsonValue(cr);
    }

    /**
     * Converts an MqlExpression to any subtype of Expression. Users must not
     * extend Expression or its subtypes, so MqlExpression will implement any R.
     */
    @SuppressWarnings("unchecked")
    <R extends Expression> R assertImplementsAllExpressions() {
        return (R) this;
    }

    protected <R extends Expression> R newMqlExpression(final Function<CodecRegistry, BsonValue> ast) {
        return new MqlExpression<>(ast).assertImplementsAllExpressions();
    }

    private <R extends Expression> R variable(final String variable) {
        return newMqlExpression((cr) -> new BsonString(variable));
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
    public <Q extends Expression> Q cond(final Q left, final Q right) {
        return newMqlExpression(ast("$cond", left, right));
    }

    /** @see DocumentExpression */

    private Function<CodecRegistry, BsonValue> getFieldInternal(final String fieldBoolean) {
        // TODO these "getFieldX" names are all very long, and longer with composite paths
        return (cr) -> astDoc("$getField", new BsonDocument().append("input", this.fn.apply(cr)).append("field", new BsonString(fieldBoolean))).apply(cr);
    }

    @Override
    public BooleanExpression getFieldBoolean(final String field) {
        return new MqlExpression<>(getFieldInternal(field));
    }

    @Override
    public NumberExpression getFieldNumber(final String field) {
        return new MqlExpression<>(getFieldInternal(field));
    }

    @Override
    public IntegerExpression getFieldInteger(final String field) {
        return new MqlExpression<>(getFieldInternal(field));
    }

    @Override
    public StringExpression getFieldString(final String field) {
        return new MqlExpression<>(getFieldInternal(field));
    }

    @Override
    public DateExpression getFieldDate(final String field) {
        return new MqlExpression<>(getFieldInternal(field));
    }

    @Override
    public DocumentExpression getFieldDocument(final String field) {
        return new MqlExpression<>(getFieldInternal(field));
    }

    @Override
    public <R extends DocumentExpression> ArrayExpression<R> getFieldArray(final String field) {
        return new MqlExpression<>(getFieldInternal(field));
    }

    @Override
    public ArrayExpression<DocumentExpression> getFieldDocumentArray(final String arrObj) {
        return new MqlExpression<>(getFieldInternal(arrObj));
    }

    @Override
    public <R extends DocumentExpression> R setField(final String path, final Expression exp) {
        return newMqlExpression((cr) -> astDoc("$setField", new BsonDocument()
                .append("field", new BsonString(path))
                .append("input", this.toBsonValue(cr))
                .append("value", extractBsonValue(cr, exp))
                .toBsonDocument(BsonDocument.class, cr)).apply(cr));
    }

    @Override
    public DocumentExpression removeField(final String path) {
        // do not expose the $$REMOVE variable
        return this.setField(path, new MqlExpression<>((cr) -> new BsonString("$$REMOVE")));
    }

    /** @see Expression */

    @Override
    public <Q extends Expression, R extends Expression> R dot(final Function<Q, R> f) {
        return f.apply(this.assertImplementsAllExpressions());
    }

    @Override
    public <R extends Expression> R ifNull(final R ifNull) {
        return new MqlExpression<>(ast("$ifNull", ifNull, Expressions.ofNull()))
                .assertImplementsAllExpressions();
    }

    @Override
    public BooleanExpression eq(final Expression eq) {
        return new MqlExpression<>(ast("$eq", eq));
    }

    @Override
    public BooleanExpression gt(final Expression gt) {
        return new MqlExpression<>(ast("$gt", gt));
    }

    @Override
    public <T0 extends Expression, R0 extends Expression, TV1 extends Expression> T0 let(
            final TV1 var1, final BiFunction<T0, TV1, R0> ex) {
        return newMqlExpression((cr) -> astDoc("$let", new BsonDocument()
                .append("vars", new BsonDocument()
                        .append("var1", extractBsonValue(cr, var1)))
                .append("in", extractBsonValue(cr, ex.apply(
                        this.assertImplementsAllExpressions(),
                        variable("$$var1"))))
                .toBsonDocument(BsonDocument.class, cr)).apply(cr));
    }

    @Override
    public <T0 extends Expression, R0 extends Expression> T0 switchMap(
            final BiFunction<T0, OptionalExpression, R0> switchMap) {
        OptionalExpression oe = new OptionalExpression();
        switchMap.apply(this.assertImplementsAllExpressions(), oe);
        return null;
    }

    /** @see ArrayExpression */

    @Override
    public <R extends Expression> ArrayExpression<R> map(final Function<T, ? extends R> in) {
        T varThis = variable("$$this");
        return new MqlExpression<>((cr) -> astDoc("$map", new BsonDocument()
                .append("input", this.toBsonValue(cr))
                .append("in", extractBsonValue(cr, in.apply(varThis)))
                .toBsonDocument(BsonDocument.class, cr)).apply(cr));
    }

    @Override
    public ArrayExpression<T> filter(final Function<T, BooleanExpression> cond) {
        T varThis = variable("$$this");
        return new MqlExpression<T>((cr) -> astDoc("$filter", new BsonDocument()
                .append("input", this.toBsonValue(cr))
                .append("cond", extractBsonValue(cr, cond.apply(varThis)))
                .toBsonDocument(BsonDocument.class, cr)).apply(cr));
    }

    @Override
    public T reduce(final T initialValue, final BiFunction<T,  T,  T> in) {
        T varThis = variable("$$this");
        T varValue = variable("$$value");
        return newMqlExpression((cr) -> astDoc("$reduce", new BsonDocument()
                .append("input", this.toBsonValue(cr))
                .append("initialValue", extractBsonValue(cr, initialValue))
                .append("in", extractBsonValue(cr, in.apply(varThis, varValue)))
                .toBsonDocument(BsonDocument.class, cr)).apply(cr));
    }


    /** @see IntegerExpression
     *  @see NumberExpression */

    @Override
    public <Q extends NumberExpression> Q multiply(final Q n) {
        return newMqlExpression(ast("$multiply", n));
    }

    @Override
    public IntegerExpression multiply(final IntegerExpression i) {
        return new MqlExpression<>(ast("$multiply", i));
    }

    @Override
    public <Q extends NumberExpression> Q abs() {
        return newMqlExpression(ast("$abs"));
    }

    @Override
    public IntegerExpression add(final IntegerExpression i) {
        return new MqlExpression<>(ast("$add", i));
    }

    @Override
    public IntegerExpression sum(final IntegerExpression i) {
        return new MqlExpression<>(ast("$sum", i));
    }

    @Override
    public IntegerExpression subtract(final IntegerExpression i) {
        return new MqlExpression<>(ast("$subtract", i));
    }

    /** @see DateExpression */

    @Override
    public IntegerExpression year() {
        return new MqlExpression<>(ast("$year"));
    }

    /** @see StringExpression */

    @Override
    public StringExpression toLower() {
        return new MqlExpression<>(ast("$toLower"));
    }
}
