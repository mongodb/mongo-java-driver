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

package com.mongodb.client.model.mql;

import com.mongodb.assertions.Assertions;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Collections;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static com.mongodb.client.model.mql.MqlValues.of;
import static com.mongodb.client.model.mql.MqlValues.ofNull;
import static com.mongodb.client.model.mql.MqlValues.ofStringArray;

final class MqlExpression<T extends MqlValue>
        implements MqlValue, MqlBoolean, MqlInteger, MqlNumber,
        MqlString, MqlDate, MqlDocument, MqlArray<T>, MqlMap<T>, MqlEntry<T> {

    private final Function<CodecRegistry, AstPlaceholder> fn;

    MqlExpression(final Function<CodecRegistry, AstPlaceholder> fn) {
        this.fn = fn;
    }

    /**
     * Exposes the evaluated BsonValue so that this mql expression may be used
     * in aggregations. Non-public, as it is intended to be used only by the
     * {@link MqlExpressionCodec}.
     */
    BsonValue toBsonValue(final CodecRegistry codecRegistry) {
        return fn.apply(codecRegistry).bsonValue;
    }

    private AstPlaceholder astDoc(final String name, final BsonDocument value) {
        return new AstPlaceholder(new BsonDocument(name, value));
    }

    @Override
    public MqlString getKey() {
        return new MqlExpression<>(getFieldInternal("k"));
    }

    @Override
    public T getValue() {
        return newMqlExpression(getFieldInternal("v"));
    }

    @Override
    public MqlEntry<T> setValue(final T value) {
        Assertions.notNull("value", value);
        return setFieldInternal("v", value);
    }

    @Override
    public MqlEntry<T> setKey(final MqlString key) {
        Assertions.notNull("key", key);
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

    private Function<CodecRegistry, AstPlaceholder> ast(final String name, final MqlValue param1) {
        return (cr) -> {
            BsonArray value = new BsonArray();
            value.add(this.toBsonValue(cr));
            value.add(toBsonValue(cr, param1));
            return new AstPlaceholder(new BsonDocument(name, value));
        };
    }

    private Function<CodecRegistry, AstPlaceholder> ast(final String name, final MqlValue param1, final MqlValue param2) {
        return (cr) -> {
            BsonArray value = new BsonArray();
            value.add(this.toBsonValue(cr));
            value.add(toBsonValue(cr, param1));
            value.add(toBsonValue(cr, param2));
            return new AstPlaceholder(new BsonDocument(name, value));
        };
    }

    /**
     * Takes an expression and converts it to a BsonValue. MqlExpression will be
     * the only implementation of Expression and all subclasses, so this will
     * not mis-cast an expression as anything else.
     */
    static BsonValue toBsonValue(final CodecRegistry cr, final MqlValue mqlValue) {
        return ((MqlExpression<?>) mqlValue).toBsonValue(cr);
    }

    /**
     * Converts an MqlExpression to any subtype of Expression. Users must not
     * extend Expression or its subtypes, so MqlExpression will implement any R.
     */
    @SuppressWarnings("unchecked")
    <R extends MqlValue> R assertImplementsAllExpressions() {
        return (R) this;
    }

    private static <R extends MqlValue> R newMqlExpression(final Function<CodecRegistry, AstPlaceholder> ast) {
        return new MqlExpression<>(ast).assertImplementsAllExpressions();
    }

    private <R extends MqlValue> R variable(final String variable) {
        return newMqlExpression((cr) -> new AstPlaceholder(new BsonString(variable)));
    }

    /** @see MqlBoolean */

    @Override
    public MqlBoolean not() {
        return new MqlExpression<>(ast("$not"));
    }

    @Override
    public MqlBoolean or(final MqlBoolean other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$or", other));
    }

    @Override
    public MqlBoolean and(final MqlBoolean other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$and", other));
    }

    @Override
    public <R extends MqlValue> R cond(final R ifTrue, final R ifFalse) {
        Assertions.notNull("ifTrue", ifTrue);
        Assertions.notNull("ifFalse", ifFalse);
        return newMqlExpression(ast("$cond", ifTrue, ifFalse));
    }

    /** @see MqlDocument */

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
    public MqlValue getField(final String fieldName) {
        Assertions.notNull("fieldName", fieldName);
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public MqlBoolean getBoolean(final String fieldName) {
        Assertions.notNull("fieldName", fieldName);
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public MqlBoolean getBoolean(final String fieldName, final MqlBoolean other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getBoolean(fieldName).isBooleanOr(other);
    }

    @Override
    public MqlNumber getNumber(final String fieldName) {
        Assertions.notNull("fieldName", fieldName);
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public MqlNumber getNumber(final String fieldName, final MqlNumber other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getNumber(fieldName).isNumberOr(other);
    }

    @Override
    public MqlInteger getInteger(final String fieldName) {
        Assertions.notNull("fieldName", fieldName);
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public MqlInteger getInteger(final String fieldName, final MqlInteger other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getInteger(fieldName).isIntegerOr(other);
    }

    @Override
    public MqlString getString(final String fieldName) {
        Assertions.notNull("fieldName", fieldName);
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public MqlString getString(final String fieldName, final MqlString other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getString(fieldName).isStringOr(other);
    }

    @Override
    public MqlDate getDate(final String fieldName) {
        Assertions.notNull("fieldName", fieldName);
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public MqlDate getDate(final String fieldName, final MqlDate other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getDate(fieldName).isDateOr(other);
    }

    @Override
    public MqlDocument getDocument(final String fieldName) {
        Assertions.notNull("fieldName", fieldName);
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public <R extends MqlValue> MqlMap<R> getMap(final String fieldName) {
        Assertions.notNull("fieldName", fieldName);
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public <R extends MqlValue> MqlMap<R> getMap(final String fieldName, final MqlMap<? extends R> other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getMap(fieldName).isMapOr(other);
    }

    @Override
    public MqlDocument getDocument(final String fieldName, final MqlDocument other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getDocument(fieldName).isDocumentOr(other);
    }

    @Override
    public <R extends MqlValue> MqlArray<R> getArray(final String fieldName) {
        Assertions.notNull("fieldName", fieldName);
        return new MqlExpression<>(getFieldInternal(fieldName));
    }

    @Override
    public <R extends MqlValue> MqlArray<R> getArray(final String fieldName, final MqlArray<? extends R> other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getArray(fieldName).isArrayOr(other);
    }

    @Override
    public MqlDocument merge(final MqlDocument other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$mergeObjects", other));
    }

    @Override
    public MqlDocument setField(final String fieldName, final MqlValue value) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("value", value);
        return setFieldInternal(fieldName, value);
    }

    private MqlExpression<T> setFieldInternal(final String fieldName, final MqlValue value) {
        Assertions.notNull("fieldName", fieldName);
        return newMqlExpression((cr) -> astDoc("$setField", new BsonDocument()
                .append("field", new BsonString(fieldName))
                .append("input", this.toBsonValue(cr))
                .append("value", toBsonValue(cr, value))));
    }

    @Override
    public MqlDocument unsetField(final String fieldName) {
        Assertions.notNull("fieldName", fieldName);
        return newMqlExpression((cr) -> astDoc("$unsetField", new BsonDocument()
                .append("field", new BsonString(fieldName))
                .append("input", this.toBsonValue(cr))));
    }

    /** @see MqlValue */

    @Override
    public <R extends MqlValue> R passTo(final Function<? super MqlValue, ? extends R> f) {
        Assertions.notNull("f", f);
        return f.apply(this.assertImplementsAllExpressions());
    }

    @Override
    public <R extends MqlValue> R switchOn(final Function<Branches<MqlValue>, ? extends BranchesTerminal<MqlValue, ? extends R>> mapping) {
        Assertions.notNull("mapping", mapping);
        return switchMapInternal(this.assertImplementsAllExpressions(), mapping.apply(new Branches<>()));
    }

    @Override
    public <R extends MqlValue> R passBooleanTo(final Function<? super MqlBoolean, ? extends R> f) {
        Assertions.notNull("f", f);
        return f.apply(this.assertImplementsAllExpressions());
    }

    @Override
    public <R extends MqlValue> R switchBooleanOn(final Function<Branches<MqlBoolean>, ? extends BranchesTerminal<MqlBoolean, ? extends R>> mapping) {
        Assertions.notNull("mapping", mapping);
        return switchMapInternal(this.assertImplementsAllExpressions(), mapping.apply(new Branches<>()));
    }

    @Override
    public <R extends MqlValue> R passIntegerTo(final Function<? super MqlInteger, ? extends R> f) {
        Assertions.notNull("f", f);
        return f.apply(this.assertImplementsAllExpressions());
    }

    @Override
    public <R extends MqlValue> R switchIntegerOn(final Function<Branches<MqlInteger>, ? extends BranchesTerminal<MqlInteger, ? extends R>> mapping) {
        Assertions.notNull("mapping", mapping);
        return switchMapInternal(this.assertImplementsAllExpressions(), mapping.apply(new Branches<>()));
    }

    @Override
    public <R extends MqlValue> R passNumberTo(final Function<? super MqlNumber, ? extends R> f) {
        Assertions.notNull("f", f);
        return f.apply(this.assertImplementsAllExpressions());
    }

    @Override
    public <R extends MqlValue> R switchNumberOn(final Function<Branches<MqlNumber>, ? extends BranchesTerminal<MqlNumber, ? extends R>> mapping) {
        Assertions.notNull("mapping", mapping);
        return switchMapInternal(this.assertImplementsAllExpressions(), mapping.apply(new Branches<>()));
    }

    @Override
    public <R extends MqlValue> R passStringTo(final Function<? super MqlString, ? extends R> f) {
        Assertions.notNull("f", f);
        return f.apply(this.assertImplementsAllExpressions());
    }

    @Override
    public <R extends MqlValue> R switchStringOn(final Function<Branches<MqlString>, ? extends BranchesTerminal<MqlString, ? extends R>> mapping) {
        Assertions.notNull("mapping", mapping);
        return switchMapInternal(this.assertImplementsAllExpressions(), mapping.apply(new Branches<>()));
    }

    @Override
    public <R extends MqlValue> R passDateTo(final Function<? super MqlDate, ? extends R> f) {
        Assertions.notNull("f", f);
        return f.apply(this.assertImplementsAllExpressions());
    }

    @Override
    public <R extends MqlValue> R switchDateOn(final Function<Branches<MqlDate>, ? extends BranchesTerminal<MqlDate, ? extends R>> mapping) {
        Assertions.notNull("mapping", mapping);
        return switchMapInternal(this.assertImplementsAllExpressions(), mapping.apply(new Branches<>()));
    }

    @Override
    public <R extends MqlValue> R passArrayTo(final Function<? super MqlArray<T>, ? extends R> f) {
        Assertions.notNull("f", f);
        return f.apply(this.assertImplementsAllExpressions());
    }

    @Override
    public <R extends MqlValue> R switchArrayOn(final Function<Branches<MqlArray<T>>, ? extends BranchesTerminal<MqlArray<T>, ? extends R>> mapping) {
        Assertions.notNull("mapping", mapping);
        return switchMapInternal(this.assertImplementsAllExpressions(), mapping.apply(new Branches<>()));
    }

    @Override
    public <R extends MqlValue> R passMapTo(final Function<? super MqlMap<T>, ? extends R> f) {
        Assertions.notNull("f", f);
        return f.apply(this.assertImplementsAllExpressions());
    }

    @Override
    public <R extends MqlValue> R switchMapOn(final Function<Branches<MqlMap<T>>, ? extends BranchesTerminal<MqlMap<T>, ? extends R>> mapping) {
        Assertions.notNull("mapping", mapping);
        return switchMapInternal(this.assertImplementsAllExpressions(), mapping.apply(new Branches<>()));
    }

    @Override
    public <R extends MqlValue> R passDocumentTo(final Function<? super MqlDocument, ? extends R> f) {
        Assertions.notNull("f", f);
        return f.apply(this.assertImplementsAllExpressions());
    }

    @Override
    public <R extends MqlValue> R switchDocumentOn(final Function<Branches<MqlDocument>, ? extends BranchesTerminal<MqlDocument, ? extends R>> mapping) {
        Assertions.notNull("mapping", mapping);
        return switchMapInternal(this.assertImplementsAllExpressions(), mapping.apply(new Branches<>()));
    }

    private <T0 extends MqlValue, R0 extends MqlValue> R0 switchMapInternal(
            final T0 value, final BranchesTerminal<T0, R0> construct) {
        return newMqlExpression((cr) -> {
            BsonArray branches = new BsonArray();
            for (Function<T0, SwitchCase<R0>> fn : construct.getBranches()) {
                SwitchCase<R0> result = fn.apply(value);
                branches.add(new BsonDocument()
                        .append("case", toBsonValue(cr, result.getCaseValue()))
                        .append("then", toBsonValue(cr, result.getThenValue())));
            }
            BsonDocument switchBson = new BsonDocument().append("branches", branches);
            if (construct.getDefaults() != null) {
                switchBson = switchBson.append("default", toBsonValue(cr, construct.getDefaults().apply(value)));
            }
            return astDoc("$switch", switchBson);
        });
    }

    @Override
    public MqlBoolean eq(final MqlValue other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$eq", other));
    }

    @Override
    public MqlBoolean ne(final MqlValue other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$ne", other));
    }

    @Override
    public MqlBoolean gt(final MqlValue other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$gt", other));
    }

    @Override
    public MqlBoolean gte(final MqlValue other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$gte", other));
    }

    @Override
    public MqlBoolean lt(final MqlValue other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$lt", other));
    }

    @Override
    public MqlBoolean lte(final MqlValue other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$lte", other));
    }

    MqlBoolean isBoolean() {
        return new MqlExpression<>(astWrapped("$type")).eq(of("bool"));
    }

    @Override
    public MqlBoolean isBooleanOr(final MqlBoolean other) {
        Assertions.notNull("other", other);
        return this.isBoolean().cond(this, other);
    }

    MqlBoolean isNumber() {
        return new MqlExpression<>(astWrapped("$isNumber"));
    }

    @Override
    public MqlNumber isNumberOr(final MqlNumber other) {
        Assertions.notNull("other", other);
        return this.isNumber().cond(this, other);
    }

    MqlBoolean isInteger() {
        return switchOn(on -> on
                .isNumber(v -> v.round().eq(v))
                .defaults(v -> of(false)));
    }

    @Override
    public MqlInteger isIntegerOr(final MqlInteger other) {
        Assertions.notNull("other", other);
        /*
        The server does not evaluate both branches of and/or/cond unless needed.
        However, the server has a pipeline optimization stage prior to
        evaluation that does attempt to optimize both branches, and fails with
        "Failed to optimize pipeline" when there is a problem arising from the
        use of literals and typed expressions. Using "switch" avoids this,
        otherwise we could just use:
        this.isNumber().and(this.eq(this.round()))
        */
        return this.switchOn(on -> on
                .isNumber(v -> (MqlInteger) v.round().eq(v).cond(v, other))
                .defaults(v -> other));
    }

    MqlBoolean isString() {
        return new MqlExpression<>(astWrapped("$type")).eq(of("string"));
    }

    @Override
    public MqlString isStringOr(final MqlString other) {
        Assertions.notNull("other", other);
        return this.isString().cond(this, other);
    }

    MqlBoolean isDate() {
        return ofStringArray("date").contains(new MqlExpression<>(astWrapped("$type")));
    }

    @Override
    public MqlDate isDateOr(final MqlDate other) {
        Assertions.notNull("other", other);
        return this.isDate().cond(this, other);
    }

    MqlBoolean isArray() {
        return new MqlExpression<>(astWrapped("$isArray"));
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
    public <R extends MqlValue> MqlArray<R> isArrayOr(final MqlArray<? extends R> other) {
        Assertions.notNull("other", other);
        return (MqlArray<R>) this.isArray().cond(this.assertImplementsAllExpressions(), other);
    }

    MqlBoolean isDocumentOrMap() {
        return new MqlExpression<>(astWrapped("$type")).eq(of("object"));
    }

    @Override
    public <R extends MqlDocument> R isDocumentOr(final R other) {
        Assertions.notNull("other", other);
        return this.isDocumentOrMap().cond(this.assertImplementsAllExpressions(), other);
    }

    @Override
    public <R extends MqlValue> MqlMap<R> isMapOr(final MqlMap<? extends R> other) {
        Assertions.notNull("other", other);
        MqlExpression<?> isMap = (MqlExpression<?>) this.isDocumentOrMap();
        return newMqlExpression(isMap.ast("$cond", this.assertImplementsAllExpressions(), other));
    }

    MqlBoolean isNull() {
        return this.eq(ofNull());
    }

    @Override
    public MqlString asString() {
        return new MqlExpression<>(astWrapped("$toString"));
    }

    private Function<CodecRegistry, AstPlaceholder> convertInternal(final String to, final MqlValue other) {
        return (cr) -> astDoc("$convert", new BsonDocument()
                .append("input", this.fn.apply(cr).bsonValue)
                .append("onError", toBsonValue(cr, other))
                .append("to", new BsonString(to)));
    }

    @Override
    public MqlInteger parseInteger() {
        MqlValue asLong = new MqlExpression<>(ast("$toLong"));
        return new MqlExpression<>(convertInternal("int", asLong));
    }

    /** @see MqlArray */

    @Override
    public <R extends MqlValue> MqlArray<R> map(final Function<? super T, ? extends R> in) {
        Assertions.notNull("in", in);
        T varThis = variable("$$this");
        return new MqlExpression<>((cr) -> astDoc("$map", new BsonDocument()
                .append("input", this.toBsonValue(cr))
                .append("in", toBsonValue(cr, in.apply(varThis)))));
    }

    @Override
    public MqlArray<T> filter(final Function<? super T, ? extends MqlBoolean> predicate) {
        Assertions.notNull("predicate", predicate);
        T varThis = variable("$$this");
        return new MqlExpression<>((cr) -> astDoc("$filter", new BsonDocument()
                .append("input", this.toBsonValue(cr))
                .append("cond", toBsonValue(cr, predicate.apply(varThis)))));
    }

    MqlArray<T> sort() {
        return new MqlExpression<>((cr) -> astDoc("$sortArray", new BsonDocument()
                .append("input", this.toBsonValue(cr))
                .append("sortBy", new BsonInt32(1))));
    }

    private T reduce(final T initialValue, final BinaryOperator<T> in) {
        T varThis = variable("$$this");
        T varValue = variable("$$value");
        return newMqlExpression((cr) -> astDoc("$reduce", new BsonDocument()
                .append("input", this.toBsonValue(cr))
                .append("initialValue", toBsonValue(cr, initialValue))
                .append("in", toBsonValue(cr, in.apply(varValue, varThis)))));
    }

    @Override
    public MqlBoolean any(final Function<? super T, MqlBoolean> predicate) {
        Assertions.notNull("predicate", predicate);
        MqlExpression<MqlBoolean> array = (MqlExpression<MqlBoolean>) this.map(predicate);
        return array.reduce(of(false), (a, b) -> a.or(b));
    }

    @Override
    public MqlBoolean all(final Function<? super T, MqlBoolean> predicate) {
        Assertions.notNull("predicate", predicate);
        MqlExpression<MqlBoolean> array = (MqlExpression<MqlBoolean>) this.map(predicate);
        return array.reduce(of(true), (a, b) -> a.and(b));
    }

    @SuppressWarnings("unchecked")
    @Override
    public MqlNumber sum(final Function<? super T, ? extends MqlNumber> mapper) {
        Assertions.notNull("mapper", mapper);
        // no sum that returns IntegerExpression, both have same erasure
        MqlExpression<MqlNumber> array = (MqlExpression<MqlNumber>) this.map(mapper);
        return array.reduce(of(0), (a, b) -> a.add(b));
    }

    @SuppressWarnings("unchecked")
    @Override
    public MqlNumber multiply(final Function<? super T, ? extends MqlNumber> mapper) {
        Assertions.notNull("mapper", mapper);
        MqlExpression<MqlNumber> array = (MqlExpression<MqlNumber>) this.map(mapper);
        return array.reduce(of(1), (MqlNumber a, MqlNumber b) -> a.multiply(b));
    }

    @Override
    public T max(final T other) {
        Assertions.notNull("other", other);
        return this.size().eq(of(0)).cond(other, this.maxN(of(1)).first());
    }

    @Override
    public T min(final T other) {
        Assertions.notNull("other", other);
        return this.size().eq(of(0)).cond(other, this.minN(of(1)).first());
    }

    @Override
    public MqlArray<T> maxN(final MqlInteger n) {
        Assertions.notNull("n", n);
        return newMqlExpression((CodecRegistry cr) -> astDoc("$maxN", new BsonDocument()
                .append("input", toBsonValue(cr, this))
                .append("n", toBsonValue(cr, n))));
    }

    @Override
    public MqlArray<T> minN(final MqlInteger n) {
        Assertions.notNull("n", n);
        return newMqlExpression((CodecRegistry cr) -> astDoc("$minN", new BsonDocument()
                .append("input", toBsonValue(cr, this))
                .append("n", toBsonValue(cr, n))));
    }

    @Override
    public MqlString joinStrings(final Function<? super T, MqlString> mapper) {
        Assertions.notNull("mapper", mapper);
        MqlExpression<MqlString> array = (MqlExpression<MqlString>) this.map(mapper);
        return array.reduce(of(""), (a, b) -> a.append(b));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R extends MqlValue> MqlArray<R> concatArrays(final Function<? super T, ? extends MqlArray<? extends R>> mapper) {
        Assertions.notNull("mapper", mapper);
        MqlExpression<MqlArray<R>> array = (MqlExpression<MqlArray<R>>) this.map(mapper);
        return array.reduce(MqlValues.ofArray(), (a, b) -> a.concat(b));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R extends MqlValue> MqlArray<R> unionArrays(final Function<? super T, ? extends MqlArray<? extends R>> mapper) {
        Assertions.notNull("mapper", mapper);
        Assertions.notNull("mapper", mapper);
        MqlExpression<MqlArray<R>> array = (MqlExpression<MqlArray<R>>) this.map(mapper);
        return array.reduce(MqlValues.ofArray(), (a, b) -> a.union(b));
    }

    @Override
    public MqlInteger size() {
        return new MqlExpression<>(astWrapped("$size"));
    }

    @Override
    public T elementAt(final MqlInteger i) {
        Assertions.notNull("i", i);
        return new MqlExpression<>(ast("$arrayElemAt", i))
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
    public MqlBoolean contains(final T value) {
        Assertions.notNull("value", value);
        String name = "$in";
        return new MqlExpression<>((cr) -> {
            BsonArray array = new BsonArray();
            array.add(toBsonValue(cr, value));
            array.add(this.toBsonValue(cr));
            return new AstPlaceholder(new BsonDocument(name, array));
        }).assertImplementsAllExpressions();
    }

    @Override
    public MqlArray<T> concat(final MqlArray<? extends T> other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$concatArrays", other))
                .assertImplementsAllExpressions();
    }

    @Override
    public MqlArray<T> slice(final MqlInteger start, final MqlInteger length) {
        Assertions.notNull("start", start);
        Assertions.notNull("length", length);
        return new MqlExpression<>(ast("$slice", start, length))
                .assertImplementsAllExpressions();
    }

    @Override
    public MqlArray<T> union(final MqlArray<? extends T> other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$setUnion", other))
                .assertImplementsAllExpressions();
    }

    @Override
    public MqlArray<T> distinct() {
        return new MqlExpression<>(astWrapped("$setUnion"));
    }


    /** @see MqlInteger
     *  @see MqlNumber */

    @Override
    public MqlInteger multiply(final MqlNumber other) {
        Assertions.notNull("other", other);
        return newMqlExpression(ast("$multiply", other));
    }

    @Override
    public MqlNumber add(final MqlNumber other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$add", other));
    }

    @Override
    public MqlNumber divide(final MqlNumber other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$divide", other));
    }

    @Override
    public MqlNumber max(final MqlNumber other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$max", other));
    }

    @Override
    public MqlNumber min(final MqlNumber other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$min", other));
    }

    @Override
    public MqlInteger round() {
        return new MqlExpression<>(ast("$round"));
    }

    @Override
    public MqlNumber round(final MqlInteger place) {
        Assertions.notNull("place", place);
        return new MqlExpression<>(ast("$round", place));
    }

    @Override
    public MqlInteger multiply(final MqlInteger other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$multiply", other));
    }

    @Override
    public MqlInteger abs() {
        return newMqlExpression(ast("$abs"));
    }

    @Override
    public MqlDate millisecondsAsDate() {
        return newMqlExpression(ast("$toDate"));
    }

    @Override
    public MqlNumber subtract(final MqlNumber other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$subtract", other));
    }

    @Override
    public MqlInteger add(final MqlInteger other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$add", other));
    }

    @Override
    public MqlInteger subtract(final MqlInteger other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$subtract", other));
    }

    @Override
    public MqlInteger max(final MqlInteger other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$max", other));
    }

    @Override
    public MqlInteger min(final MqlInteger other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$min", other));
    }

    /** @see MqlDate */

    private MqlExpression<MqlValue> usingTimezone(final String name, final MqlString timezone) {
        return new MqlExpression<>((cr) -> astDoc(name, new BsonDocument()
                .append("date", this.toBsonValue(cr))
                .append("timezone", toBsonValue(cr, timezone))));
    }

    @Override
    public MqlInteger year(final MqlString timezone) {
        Assertions.notNull("timezone", timezone);
        return usingTimezone("$year", timezone);
    }

    @Override
    public MqlInteger month(final MqlString timezone) {
        Assertions.notNull("timezone", timezone);
        return usingTimezone("$month", timezone);
    }

    @Override
    public MqlInteger dayOfMonth(final MqlString timezone) {
        Assertions.notNull("timezone", timezone);
        return usingTimezone("$dayOfMonth", timezone);
    }

    @Override
    public MqlInteger dayOfWeek(final MqlString timezone) {
        Assertions.notNull("timezone", timezone);
        return usingTimezone("$dayOfWeek", timezone);
    }

    @Override
    public MqlInteger dayOfYear(final MqlString timezone) {
        Assertions.notNull("timezone", timezone);
        return usingTimezone("$dayOfYear", timezone);
    }

    @Override
    public MqlInteger hour(final MqlString timezone) {
        Assertions.notNull("timezone", timezone);
        return usingTimezone("$hour", timezone);
    }

    @Override
    public MqlInteger minute(final MqlString timezone) {
        Assertions.notNull("timezone", timezone);
        return usingTimezone("$minute", timezone);
    }

    @Override
    public MqlInteger second(final MqlString timezone) {
        Assertions.notNull("timezone", timezone);
        return usingTimezone("$second", timezone);
    }

    @Override
    public MqlInteger week(final MqlString timezone) {
        Assertions.notNull("timezone", timezone);
        return usingTimezone("$week", timezone);
    }

    @Override
    public MqlInteger millisecond(final MqlString timezone) {
        Assertions.notNull("timezone", timezone);
        return usingTimezone("$millisecond", timezone);
    }

    @Override
    public MqlString asString(final MqlString timezone, final MqlString format) {
        Assertions.notNull("timezone", timezone);
        Assertions.notNull("format", format);
        return newMqlExpression((cr) -> astDoc("$dateToString", new BsonDocument()
                .append("date", this.toBsonValue(cr))
                .append("format", toBsonValue(cr, format))
                .append("timezone", toBsonValue(cr, timezone))));
    }

    @Override
    public MqlDate parseDate(final MqlString timezone, final MqlString format) {
        Assertions.notNull("timezone", timezone);
        Assertions.notNull("format", format);
        return newMqlExpression((cr) -> astDoc("$dateFromString", new BsonDocument()
                .append("dateString", this.toBsonValue(cr))
                .append("format", toBsonValue(cr, format))
                .append("timezone", toBsonValue(cr, timezone))));
    }

    @Override
    public MqlDate parseDate(final MqlString format) {
        Assertions.notNull("format", format);
        return newMqlExpression((cr) -> astDoc("$dateFromString", new BsonDocument()
                .append("dateString", this.toBsonValue(cr))
                .append("format", toBsonValue(cr, format))));
    }

    @Override
    public MqlDate parseDate() {
        return newMqlExpression((cr) -> astDoc("$dateFromString", new BsonDocument()
                .append("dateString", this.toBsonValue(cr))));
    }

    /** @see MqlString */

    @Override
    public MqlString toLower() {
        return new MqlExpression<>(ast("$toLower"));
    }

    @Override
    public MqlString toUpper() {
        return new MqlExpression<>(ast("$toUpper"));
    }

    @Override
    public MqlString append(final MqlString other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$concat", other));
    }

    @Override
    public MqlInteger length() {
        return new MqlExpression<>(ast("$strLenCP"));
    }

    @Override
    public MqlInteger lengthBytes() {
        return new MqlExpression<>(ast("$strLenBytes"));
    }

    @Override
    public MqlString substr(final MqlInteger start, final MqlInteger length) {
        Assertions.notNull("start", start);
        Assertions.notNull("length", length);
        return new MqlExpression<>(ast("$substrCP", start, length));
    }

    @Override
    public MqlString substrBytes(final MqlInteger start, final MqlInteger length) {
        Assertions.notNull("start", start);
        Assertions.notNull("length", length);
        return new MqlExpression<>(ast("$substrBytes", start, length));
    }

    @Override
    public MqlBoolean has(final MqlString key) {
        Assertions.notNull("key", key);
        return get(key).ne(ofRem());
    }


    @Override
    public MqlBoolean hasField(final String fieldName) {
        Assertions.notNull("fieldName", fieldName);
        return this.has(of(fieldName));
    }

    static <R extends MqlValue> R ofRem() {
        // $$REMOVE is intentionally not exposed to users
        return new MqlExpression<>((cr) -> new MqlExpression.AstPlaceholder(new BsonString("$$REMOVE")))
                .assertImplementsAllExpressions();
    }

    /** @see MqlMap
     * @see MqlEntry */

    @Override
    public T get(final MqlString key) {
        Assertions.notNull("key", key);
        return newMqlExpression((cr) -> astDoc("$getField", new BsonDocument()
                .append("input", this.fn.apply(cr).bsonValue)
                .append("field", toBsonValue(cr, key))));
    }

    @SuppressWarnings("unchecked")
    @Override
    public T get(final MqlString key, final T other) {
        Assertions.notNull("key", key);
        Assertions.notNull("other", other);
        MqlExpression<?> mqlExpression = (MqlExpression<?>) get(key);
        return (T) mqlExpression.eq(ofRem()).cond(other, mqlExpression);
    }

    @Override
    public MqlMap<T> set(final MqlString key, final T value) {
        Assertions.notNull("key", key);
        Assertions.notNull("value", value);
        return newMqlExpression((cr) -> astDoc("$setField", new BsonDocument()
                .append("field", toBsonValue(cr, key))
                .append("input", this.toBsonValue(cr))
                .append("value", toBsonValue(cr, value))));
    }

    @Override
    public MqlMap<T> unset(final MqlString key) {
        Assertions.notNull("key", key);
        return newMqlExpression((cr) -> astDoc("$unsetField", new BsonDocument()
                .append("field", toBsonValue(cr, key))
                .append("input", this.toBsonValue(cr))));
    }

    @Override
    public MqlMap<T> merge(final MqlMap<? extends T> other) {
        Assertions.notNull("other", other);
        return new MqlExpression<>(ast("$mergeObjects", other));
    }

    @Override
    public MqlArray<MqlEntry<T>> entries() {
        return newMqlExpression(ast("$objectToArray"));
    }

    @Override
    public <R extends MqlValue> MqlMap<R> asMap(
            final Function<? super T, ? extends MqlEntry<? extends R>> mapper) {
        Assertions.notNull("mapper", mapper);
        @SuppressWarnings("unchecked")
        MqlExpression<MqlEntry<? extends R>> array = (MqlExpression<MqlEntry<? extends R>>) this.map(mapper);
        return newMqlExpression(array.astWrapped("$arrayToObject"));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Q extends MqlValue> MqlMap<Q> asMap() {
        return (MqlMap<Q>) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R extends MqlDocument> R asDocument() {
        return (R) this;
    }
}
