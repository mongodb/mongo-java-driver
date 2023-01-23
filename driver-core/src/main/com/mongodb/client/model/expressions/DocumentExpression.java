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

import com.mongodb.annotations.Beta;
import com.mongodb.annotations.Sealed;
import com.mongodb.assertions.Assertions;
import org.bson.conversions.Bson;

import java.time.Instant;
import java.util.function.Function;

import static com.mongodb.client.model.expressions.Expressions.of;
import static com.mongodb.client.model.expressions.Expressions.ofMap;
import static com.mongodb.client.model.expressions.MqlUnchecked.Unchecked.PRESENT;
import static com.mongodb.client.model.expressions.MqlUnchecked.Unchecked.TYPE;
import static com.mongodb.client.model.expressions.MqlUnchecked.Unchecked.TYPE_ARGUMENT;

/**
 * A document {@link Expression value} in the context of the MongoDB Query
 * Language (MQL). A document is a finite set of fields, where the field
 * name is a string, together with a value of any other
 * {@linkplain Expression type in the type hierarchy}.
 * No field name is repeated.
 *
 * @since 4.9.0
 */
@Sealed
@Beta(Beta.Reason.CLIENT)
public interface DocumentExpression extends Expression {

    /**
     * Whether {@code this} document has a field with the provided
     * {@code fieldName} (if a field is set to null, it is present).
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @return the resulting value.
     */
    BooleanExpression has(String fieldName);

    /**
     * Returns a document with the same fields as {@code this} document, but
     * with the {@code fieldName} field set to the specified {@code value}.
     *
     * <p>This does not affect the original document.
     *
     * <p>Warning: Users should take care to assign values, such that the types
     * of those values correspond to the types of ensuing {@code get...}
     * invocations, since this API has no way of verifying this correspondence.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param value the value.
     * @return the resulting document.
     */
    DocumentExpression setField(String fieldName, Expression value);

    /**
     * Returns a document with the same fields as {@code this} document, but
     * excluding the field with the specified {@code fieldName}.
     *
     * <p>This does not affect the original document.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @return the resulting document.
     */
    DocumentExpression unsetField(String fieldName);

    /**
     * Returns the {@linkplain Expression} value of the field
     * with the provided {@code fieldName}.
     *
     * <p>Warning: Use of this method is an assertion that the document
     * {@linkplain #has(String) has} the named field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @return the resulting value.
     */
    @MqlUnchecked(PRESENT)
    Expression getField(String fieldName);

    /**
     * Returns the {@linkplain BooleanExpression boolean} value of the field
     * with the provided {@code fieldName}.
     *
     * <p>Warning: The type and presence of the resulting value is not
     * enforced by the API. The use of this method is an
     * unchecked assertion that the document
     * {@linkplain #has(String) has} the named field and
     * the field value is of the specified type.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @return the resulting value.
     */
    @MqlUnchecked({PRESENT, TYPE})
    BooleanExpression getBoolean(String fieldName);

    /**
     * Returns the {@linkplain BooleanExpression boolean} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not a boolean
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    BooleanExpression getBoolean(String fieldName, BooleanExpression other);

    /**
     * Returns the {@linkplain BooleanExpression boolean} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not a boolean
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    default BooleanExpression getBoolean(final String fieldName, final boolean other) {
        Assertions.notNull("fieldName", fieldName);
        return getBoolean(fieldName, of(other));
    }

    /**
     * Returns the {@linkplain NumberExpression number} value of the field
     * with the provided {@code fieldName}.
     *
     * <p>Warning: The type and presence of the resulting value is not
     * enforced by the API. The use of this method is an
     * unchecked assertion that the document
     * {@linkplain #has(String) has} the named field and
     * the field value is of the specified type.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @return the resulting value.
     */
    @MqlUnchecked({PRESENT, TYPE})
    NumberExpression getNumber(String fieldName);

    /**
     * Returns the {@linkplain NumberExpression number} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not a number
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    NumberExpression getNumber(String fieldName, NumberExpression other);

    /**
     * Returns the {@linkplain NumberExpression number} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not a number
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    default NumberExpression getNumber(final String fieldName, final Number other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getNumber(fieldName, Expressions.numberToExpression(other));
    }

    /**
     * Returns the {@linkplain IntegerExpression integer} value of the field
     * with the provided {@code fieldName}.
     *
     * <p>Warning: The type and presence of the resulting value is not
     * enforced by the API. The use of this method is an
     * unchecked assertion that the document
     * {@linkplain #has(String) has} the named field and
     * the field value is of the specified type.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @return the resulting value.
     */
    @MqlUnchecked({PRESENT, TYPE})
    IntegerExpression getInteger(String fieldName);

    /**
     * Returns the {@linkplain IntegerExpression integer} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not an integer
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    IntegerExpression getInteger(String fieldName, IntegerExpression other);

    /**
     * Returns the {@linkplain IntegerExpression integer} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not an integer
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    default IntegerExpression getInteger(final String fieldName, final int other) {
        Assertions.notNull("fieldName", fieldName);
        return getInteger(fieldName, of(other));
    }

    /**
     * Returns the {@linkplain IntegerExpression integer} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not an integer
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    default IntegerExpression getInteger(final String fieldName, final long other) {
        Assertions.notNull("fieldName", fieldName);
        return getInteger(fieldName, of(other));
    }

    /**
     * Returns the {@linkplain StringExpression string} value of the field
     * with the provided {@code fieldName}.
     *
     * <p>Warning: The type and presence of the resulting value is not
     * enforced by the API. The use of this method is an
     * unchecked assertion that the document
     * {@linkplain #has(String) has} the named field and
     * the field value is of the specified type.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @return the resulting value.
     */
    @MqlUnchecked({PRESENT, TYPE})
    StringExpression getString(String fieldName);

    /**
     * Returns the {@linkplain StringExpression string} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not a string
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    StringExpression getString(String fieldName, StringExpression other);

    /**
     * Returns the {@linkplain StringExpression string} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not a string
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    default StringExpression getString(final String fieldName, final String other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getString(fieldName, of(other));
    }

    /**
     * Returns the {@linkplain DateExpression date} value of the field
     * with the provided {@code fieldName}.
     *
     * <p>Warning: The type and presence of the resulting value is not
     * enforced by the API. The use of this method is an
     * unchecked assertion that the document
     * {@linkplain #has(String) has} the named field and
     * the field value is of the specified type.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @return the resulting value.
     */
    @MqlUnchecked({PRESENT, TYPE})
    DateExpression getDate(String fieldName);

    /**
     * Returns the {@linkplain DateExpression date} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not a date
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    DateExpression getDate(String fieldName, DateExpression other);

    /**
     * Returns the {@linkplain DateExpression date} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not a date
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    default DateExpression getDate(final String fieldName, final Instant other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getDate(fieldName, of(other));
    }

    /**
     * Returns the {@linkplain DocumentExpression document} value of the field
     * with the provided {@code fieldName}.
     *
     * <p>Warning: The type and presence of the resulting value is not
     * enforced by the API. The use of this method is an
     * unchecked assertion that the document
     * {@linkplain #has(String) has} the named field and
     * the field value is of the specified type.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @return the resulting value.
     */
    @MqlUnchecked({PRESENT, TYPE})
    DocumentExpression getDocument(String fieldName);

    /**
     * Returns the {@linkplain DocumentExpression document} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value
     * if the document {@linkplain #has} no such field,
     * or if the specified field is not a (child) document
     * (or other {@linkplain Expression#isDocumentOr document-like value}.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    DocumentExpression getDocument(String fieldName, DocumentExpression other);

    /**
     * Returns the {@linkplain DocumentExpression document} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value
     * if the document {@linkplain #has} no such field,
     * or if the specified field is not a (child) document
     * (or other {@linkplain Expression#isDocumentOr document-like value}.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    default DocumentExpression getDocument(final String fieldName, final Bson other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getDocument(fieldName, of(other));
    }

    /**
     * Returns the {@linkplain MapExpression map} value of the field
     * with the provided {@code fieldName}.
     *
     * <p>Warning: The type and presence of the resulting value is not
     * enforced by the API. The use of this method is an
     * unchecked assertion that the document
     * {@linkplain #has(String) has} the named field,
     * and the field value is of the specified raw type,
     * and the field value's type has the specified type argument.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @return the resulting value.
     * @param <T> the type.
     */
    @MqlUnchecked({PRESENT, TYPE})
    <T extends Expression> MapExpression<@MqlUnchecked(TYPE_ARGUMENT) T> getMap(String fieldName);


    /**
     * Returns the {@linkplain MapExpression map} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value
     * if the document {@linkplain #has} no such field,
     * or if the specified field is not a map
     * (or other {@linkplain Expression#isMapOr} map-like value}).
     *
     * <p>Warning: The type argument of the resulting value is not
     * enforced by the API. The use of this method is an
     * unchecked assertion that the type argument is correct.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     * @param <T> the type.
     */
    <T extends Expression> MapExpression<T> getMap(String fieldName, MapExpression<@MqlUnchecked(TYPE_ARGUMENT) ? extends T> other);

    /**
     * Returns the {@linkplain MapExpression map} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value
     * if the document {@linkplain #has} no such field,
     * or if the specified field is not a map
     * (or other {@linkplain Expression#isMapOr} map-like value}).
     *
     * <p>Warning: The type argument of the resulting value is not
     * enforced by the API. The use of this method is an
     * unchecked assertion that the type argument is correct.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     * @param <T> the type.
     */
    default <T extends Expression> MapExpression<@MqlUnchecked(TYPE_ARGUMENT) T> getMap(final String fieldName, final Bson other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getMap(fieldName, ofMap(other));
    }

    /**
     * Returns the {@linkplain ArrayExpression array} value of the field
     * with the provided {@code fieldName}.
     *
     * <p>Warning: The type and presence of the resulting value is not
     * enforced by the API. The use of this method is an
     * unchecked assertion that the document
     * {@linkplain #has(String) has} the named field,
     * and the field value is of the specified raw type,
     * and the field value's type has the specified type argument.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @return the resulting value.
     * @param <T> the type.
     */
    @MqlUnchecked({PRESENT, TYPE})
    <T extends Expression> ArrayExpression<@MqlUnchecked(TYPE_ARGUMENT) T> getArray(String fieldName);

    /**
     * Returns the {@linkplain ArrayExpression array} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not an array
     * or if the document {@linkplain #has} no such field.
     *
     * <p>Warning: The type argument of the resulting value is not
     * enforced by the API. The use of this method is an
     * unchecked assertion that the type argument is correct.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     * @param <T> the type.
     */
    <T extends Expression> ArrayExpression<@MqlUnchecked(TYPE_ARGUMENT) T> getArray(String fieldName, ArrayExpression<? extends T> other);

    /**
     * Returns a document with the same fields as {@code this} document, but
     * with any fields present in the {@code other} document overwritten with
     * the fields of that other document. That is, fields from both this and the
     * other document are merged, with the other document having priority.
     *
     * <p>This does not affect the original document.
     *
     * @param other the other document.
     * @return the resulting value.
     */
    DocumentExpression merge(DocumentExpression other);

    /**
     * {@code this} document as a {@linkplain MapExpression map}.
     *
     * <p>Warning: The type argument of the resulting value is not
     * enforced by the API. The use of this method is an
     * unchecked assertion that the type argument is correct.
     *
     * @return the resulting value.
     * @param <T> the type.
     */

    <T extends Expression> MapExpression<T> asMap();

    /**
     * The result of passing {@code this} value to the provided function.
     * Equivalent to {@code f.apply(this)}, and allows lambdas and static,
     * user-defined functions to use the chaining syntax.
     *
     * @see Expression#passTo
     * @param f the function to apply.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends Expression> R passDocumentTo(Function<? super DocumentExpression, ? extends R> f);

    /**
     * The result of applying the provided switch mapping to {@code this} value.
     *
     * @see Expression#switchOn
     * @param mapping the switch mapping.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends Expression> R switchDocumentOn(Function<Branches<DocumentExpression>, ? extends BranchesTerminal<DocumentExpression, ? extends R>> mapping);
}
