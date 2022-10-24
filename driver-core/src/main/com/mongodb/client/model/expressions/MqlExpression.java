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

}
