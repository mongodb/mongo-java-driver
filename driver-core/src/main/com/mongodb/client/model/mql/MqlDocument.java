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

import com.mongodb.annotations.Beta;
import com.mongodb.annotations.Sealed;
import com.mongodb.assertions.Assertions;
import org.bson.conversions.Bson;

import java.time.Instant;
import java.util.function.Function;

import static com.mongodb.client.model.mql.MqlValues.of;
import static com.mongodb.client.model.mql.MqlValues.ofMap;
import static com.mongodb.client.model.mql.MqlUnchecked.Unchecked.PRESENT;
import static com.mongodb.client.model.mql.MqlUnchecked.Unchecked.TYPE;
import static com.mongodb.client.model.mql.MqlUnchecked.Unchecked.TYPE_ARGUMENT;

/**
 * A document {@link MqlValue value} in the context of the MongoDB Query
 * Language (MQL). A document is a finite set of fields, where the field
 * name is a string, together with a value of any other
 * {@linkplain MqlValue type in the type hierarchy}.
 * No field name is repeated.
 *
 * @since 4.9.0
 */
@Sealed
@Beta(Beta.Reason.CLIENT)
public interface MqlDocument extends MqlValue {

    /**
     * Whether {@code this} document has a field with the provided
     * {@code fieldName} (if a field is set to null, it is present).
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @return the resulting value.
     */
    MqlBoolean has(String fieldName);

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
    MqlDocument setField(String fieldName, MqlValue value);

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
    MqlDocument unsetField(String fieldName);

    /**
     * Returns the {@linkplain MqlValue} value of the field
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
    MqlValue getField(String fieldName);

    /**
     * Returns the {@linkplain MqlBoolean boolean} value of the field
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
    MqlBoolean getBoolean(String fieldName);

    /**
     * Returns the {@linkplain MqlBoolean boolean} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not a boolean
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    MqlBoolean getBoolean(String fieldName, MqlBoolean other);

    /**
     * Returns the {@linkplain MqlBoolean boolean} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not a boolean
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    default MqlBoolean getBoolean(final String fieldName, final boolean other) {
        Assertions.notNull("fieldName", fieldName);
        return getBoolean(fieldName, of(other));
    }

    /**
     * Returns the {@linkplain MqlNumber number} value of the field
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
    MqlNumber getNumber(String fieldName);

    /**
     * Returns the {@linkplain MqlNumber number} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not a number
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    MqlNumber getNumber(String fieldName, MqlNumber other);

    /**
     * Returns the {@linkplain MqlNumber number} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not a number
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    default MqlNumber getNumber(final String fieldName, final Number other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getNumber(fieldName, MqlValues.numberToExpression(other));
    }

    /**
     * Returns the {@linkplain MqlInteger integer} value of the field
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
    MqlInteger getInteger(String fieldName);

    /**
     * Returns the {@linkplain MqlInteger integer} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not an integer
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    MqlInteger getInteger(String fieldName, MqlInteger other);

    /**
     * Returns the {@linkplain MqlInteger integer} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not an integer
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    default MqlInteger getInteger(final String fieldName, final int other) {
        Assertions.notNull("fieldName", fieldName);
        return getInteger(fieldName, of(other));
    }

    /**
     * Returns the {@linkplain MqlInteger integer} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not an integer
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    default MqlInteger getInteger(final String fieldName, final long other) {
        Assertions.notNull("fieldName", fieldName);
        return getInteger(fieldName, of(other));
    }

    /**
     * Returns the {@linkplain MqlString string} value of the field
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
    MqlString getString(String fieldName);

    /**
     * Returns the {@linkplain MqlString string} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not a string
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    MqlString getString(String fieldName, MqlString other);

    /**
     * Returns the {@linkplain MqlString string} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not a string
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    default MqlString getString(final String fieldName, final String other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getString(fieldName, of(other));
    }

    /**
     * Returns the {@linkplain MqlDate date} value of the field
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
    MqlDate getDate(String fieldName);

    /**
     * Returns the {@linkplain MqlDate date} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not a date
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    MqlDate getDate(String fieldName, MqlDate other);

    /**
     * Returns the {@linkplain MqlDate date} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value if the field is not a date
     * or if the document {@linkplain #has} no such field.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    default MqlDate getDate(final String fieldName, final Instant other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getDate(fieldName, of(other));
    }

    /**
     * Returns the {@linkplain MqlDocument document} value of the field
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
    MqlDocument getDocument(String fieldName);

    /**
     * Returns the {@linkplain MqlDocument document} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value
     * if the document {@linkplain #has} no such field,
     * or if the specified field is not a (child) document
     * (or other {@linkplain MqlValue#isDocumentOr document-like value}.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    MqlDocument getDocument(String fieldName, MqlDocument other);

    /**
     * Returns the {@linkplain MqlDocument document} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value
     * if the document {@linkplain #has} no such field,
     * or if the specified field is not a (child) document
     * (or other {@linkplain MqlValue#isDocumentOr document-like value}.
     *
     * @mongodb.server.release 5.0
     * @param fieldName the name of the field.
     * @param other the other value.
     * @return the resulting value.
     */
    default MqlDocument getDocument(final String fieldName, final Bson other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getDocument(fieldName, of(other));
    }

    /**
     * Returns the {@linkplain MqlMap map} value of the field
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
    <T extends MqlValue> MqlMap<@MqlUnchecked(TYPE_ARGUMENT) T> getMap(String fieldName);


    /**
     * Returns the {@linkplain MqlMap map} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value
     * if the document {@linkplain #has} no such field,
     * or if the specified field is not a map
     * (or other {@linkplain MqlValue#isMapOr} map-like value}).
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
    <T extends MqlValue> MqlMap<T> getMap(String fieldName, MqlMap<@MqlUnchecked(TYPE_ARGUMENT) ? extends T> other);

    /**
     * Returns the {@linkplain MqlMap map} value of the field
     * with the provided {@code fieldName},
     * or the {@code other} value
     * if the document {@linkplain #has} no such field,
     * or if the specified field is not a map
     * (or other {@linkplain MqlValue#isMapOr} map-like value}).
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
    default <T extends MqlValue> MqlMap<@MqlUnchecked(TYPE_ARGUMENT) T> getMap(final String fieldName, final Bson other) {
        Assertions.notNull("fieldName", fieldName);
        Assertions.notNull("other", other);
        return getMap(fieldName, ofMap(other));
    }

    /**
     * Returns the {@linkplain MqlArray array} value of the field
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
    <T extends MqlValue> MqlArray<@MqlUnchecked(TYPE_ARGUMENT) T> getArray(String fieldName);

    /**
     * Returns the {@linkplain MqlArray array} value of the field
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
    <T extends MqlValue> MqlArray<@MqlUnchecked(TYPE_ARGUMENT) T> getArray(String fieldName, MqlArray<? extends T> other);

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
    MqlDocument merge(MqlDocument other);

    /**
     * {@code this} document as a {@linkplain MqlMap map}.
     *
     * <p>Warning: The type argument of the resulting value is not
     * enforced by the API. The use of this method is an
     * unchecked assertion that the type argument is correct.
     *
     * @return the resulting value.
     * @param <T> the type.
     */

    <T extends MqlValue> MqlMap<T> asMap();

    /**
     * The result of passing {@code this} value to the provided function.
     * Equivalent to {@code f.apply(this)}, and allows lambdas and static,
     * user-defined functions to use the chaining syntax.
     *
     * @see MqlValue#passTo
     * @param f the function to apply.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends MqlValue> R passDocumentTo(Function<? super MqlDocument, ? extends R> f);

    /**
     * The result of applying the provided switch mapping to {@code this} value.
     *
     * @see MqlValue#switchOn
     * @param mapping the switch mapping.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends MqlValue> R switchDocumentOn(Function<Branches<MqlDocument>, ? extends BranchesTerminal<MqlDocument, ? extends R>> mapping);
}
