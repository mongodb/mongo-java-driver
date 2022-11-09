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

    static class MqlWrappingExpression<T extends Expression> implements Expression {
        private final MqlExpression<T> wrapped;

        private MqlWrappingExpression(final MqlExpression<T> wrapped) {
            this.wrapped = wrapped;
        }

        public MqlExpression<T> getWrapped() {
            return wrapped;
        }
    }

    static class MqlBooleanWrappingExpression<T extends Expression> extends MqlWrappingExpression<T> implements BooleanExpression {
        MqlBooleanWrappingExpression(final MqlExpression<T> wrapped) {
            super(wrapped);
        }

        @Override
        public BooleanExpression not() {
            return getWrapped().not();
        }

        @Override
        public BooleanExpression or(final BooleanExpression or) {
            return getWrapped().or(or);
        }

        @Override
        public BooleanExpression and(final BooleanExpression and) {
            return getWrapped().and(and);
        }

        @Override
        public <R extends Expression> R cond(final R left, final R right) {
            return getWrapped().cond(left, right);
        }
    }

    static class MqlNumberWrappingExpression<T extends Expression> extends MqlWrappingExpression<T> implements NumberExpression {
        MqlNumberWrappingExpression(final MqlExpression<T> wrapped) {
            super(wrapped);
        }
    }

    static class MqlIntegerWrappingExpression<T extends Expression> extends MqlWrappingExpression<T> implements IntegerExpression {
        MqlIntegerWrappingExpression(final MqlExpression<T> wrapped) {
            super(wrapped);
        }
    }

    static class MqlStringWrappingExpression<T extends Expression> extends MqlWrappingExpression<T> implements StringExpression {
        MqlStringWrappingExpression(final MqlExpression<T> wrapped) {
            super(wrapped);
        }
    }

    static class MqlArrayWrappingExpression<T extends Expression> extends MqlWrappingExpression<T> implements ArrayExpression<T> {
        MqlArrayWrappingExpression(final MqlExpression<T> wrapped) {
            super(wrapped);
        }

        @Override
        public ArrayExpression<T> filter(final Function<? super T, ? extends BooleanExpression> cond) {
            return getWrapped().filter(cond);
        }

        @Override
        public <R extends Expression> ArrayExpression<R> map(final Function<? super T, ? extends R> in) {
            return getWrapped().map(in);
        }

        @Override
        public T reduce(final T initialValue, final BinaryOperator<T> in) {
            return getWrapped().reduce(initialValue, in);
        }
    }


    private final Function<CodecRegistry, BsonValue> fn;

    MqlExpression(final Function<CodecRegistry, BsonValue> fn) {
        this.fn = fn;
    }

    /**
     * Exposes the evaluated BsonValue so that expressions may be used in
     * aggregations. Non-public, as it is intended to be used only by the
     * {@link MqlExpressionCodec}.
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
    private static BsonValue extractBsonValue(final CodecRegistry cr, final Expression expression) {
        if (expression instanceof MqlExpression.MqlWrappingExpression) {
            return ((MqlWrappingExpression) expression).getWrapped().toBsonValue(cr);
        }
        return ((MqlExpression<?>) expression).toBsonValue(cr);
    }

    /**
     * Converts an MqlExpression to any subtype of Expression. Users must not
     * extend Expression or its subtypes, so MqlExpression will implement any R.
     */
    @SuppressWarnings("unchecked")
    private <R extends Expression> R assertImplementsAllExpressions() {
        return (R) this;
    }

    private static <R extends Expression> R newMqlExpression(final Function<CodecRegistry, BsonValue> ast) {
        return new MqlExpression<>(ast).assertImplementsAllExpressions();
    }

    private <R extends Expression> R variable(final String variable) {
        return newMqlExpression((cr) -> new BsonString(variable));
    }

    /**
     * @see BooleanExpression
     */

    @Override
    public BooleanExpression not() {
        return new MqlBooleanWrappingExpression<>(new MqlExpression<>(ast("$not")));
    }

    @Override
    public BooleanExpression or(final BooleanExpression or) {
        return new MqlBooleanWrappingExpression<>(new MqlExpression<>(ast("$or", or)));
    }

    @Override
    public BooleanExpression and(final BooleanExpression and) {
        return new MqlBooleanWrappingExpression<>(new MqlExpression<>(ast("$and", and)));
    }

    @Override
    public <R extends Expression> R cond(final R left, final R right) {
        return wrap(left, right, new MqlExpression<>(ast("$cond", left, right)));
    }

    /**
     * @see ArrayExpression
     */

    @Override
    public <R extends Expression> ArrayExpression<R> map(final Function<? super T, ? extends R> in) {
        T varThis = variable("$$this");
        return new MqlArrayWrappingExpression<>(new MqlExpression<>((cr) -> astDoc("$map", new BsonDocument()
                .append("input", this.toBsonValue(cr))
                .append("in", extractBsonValue(cr, in.apply(varThis)))).apply(cr)));
    }

    @Override
    public ArrayExpression<T> filter(final Function<? super T, ? extends BooleanExpression> cond) {
        T varThis = variable("$$this");
        return new MqlArrayWrappingExpression<>(new MqlExpression<>((cr) -> astDoc("$filter", new BsonDocument()
                .append("input", this.toBsonValue(cr))
                .append("cond", extractBsonValue(cr, cond.apply(varThis)))).apply(cr)));
    }

    @Override
    public T reduce(final T initialValue, final BinaryOperator<T> in) {
        T varThis = variable("$$this");
        T varValue = variable("$$value");
        MqlExpression<T> mqlExpression = new MqlExpression<>((cr) -> {
            Function<CodecRegistry, BsonValue> ast = astDoc("$reduce", new BsonDocument()
                    .append("input", this.toBsonValue(cr))
                    .append("initialValue", extractBsonValue(cr, initialValue))
                    .append("in", extractBsonValue(cr, in.apply(varThis, varValue))));
            return ast.apply(cr);
        });
        // NOTE: saved by the presence of initialValue
        return wrap(initialValue, mqlExpression);
    }

    // NOTE: we're forced to reimplement the parameterized type system here in code

    @SuppressWarnings("unchecked")
    private static <R extends Expression> R wrap(R left, R right, MqlExpression<Expression> wrapped) {
        if (left instanceof IntegerExpression && right instanceof IntegerExpression) {
            return (R) new MqlIntegerWrappingExpression<>(wrapped);
        }
        if (left instanceof NumberExpression && right instanceof NumberExpression) {
            return (R) new MqlNumberWrappingExpression<>(wrapped);
        }
        if (left instanceof BooleanExpression && right instanceof BooleanExpression) {
            return (R) new MqlNumberWrappingExpression<>(wrapped);
        }
        if (left instanceof StringExpression && right instanceof StringExpression) {
            return (R) new MqlStringWrappingExpression<>(wrapped);
        }
        if (left instanceof ArrayExpression && right instanceof ArrayExpression) {
            return (R) new MqlArrayWrappingExpression<>(wrapped);
        }

        return (R) new MqlWrappingExpression<>(wrapped);
    }

    @SuppressWarnings("unchecked")
    private static <R extends Expression> R wrap(R expressionInstance, MqlExpression<R> wrapped) {
        if (expressionInstance instanceof IntegerExpression) {
            return (R) new MqlIntegerWrappingExpression<>(wrapped);
        }
        if (expressionInstance instanceof NumberExpression) {
            return (R) new MqlNumberWrappingExpression<>(wrapped);
        }
        if (expressionInstance instanceof BooleanExpression) {
            return (R) new MqlNumberWrappingExpression<>(wrapped);
        }
        if (expressionInstance instanceof StringExpression) {
            return (R) new MqlStringWrappingExpression<>(wrapped);
        }
        if (expressionInstance instanceof ArrayExpression) {
            return (R) new MqlArrayWrappingExpression<>(wrapped);
        }

        return (R) new MqlWrappingExpression<>(wrapped);
    }
}
