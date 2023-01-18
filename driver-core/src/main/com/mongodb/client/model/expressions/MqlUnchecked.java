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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Documents places where the API relies on a user asserting
 * something that is not checked at run-time.
 * If the assertion turns out to be false, the API behavior is unspecified.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.METHOD, ElementType.TYPE_USE})
public @interface MqlUnchecked {
    /**
     * @return A hint on the user assertion the API relies on.
     */
    Unchecked[] value();

    /**
     * @see MqlUnchecked#value()
     */
    enum Unchecked {
        /**
         * The API relies on the values it encounters being of the type
         * implied/specified by or inferred from the user code.
         * For example, {@link com.mongodb.client.model.expressions.DocumentExpression#getBoolean(String)}
         * relies on the values of the document field being of the
         * {@linkplain com.mongodb.client.model.expressions.BooleanExpression boolean} type.
         */
        TYPE,
        /**
         * The API checks the raw type, but relies on the type argument
         * implied/specified by or inferred from the user code being correct.
         * For example, {@link com.mongodb.client.model.expressions.Expression#isArrayOr(ArrayExpression)}
         * checks that the value is of the
         * {@linkplain com.mongodb.client.model.expressions.ArrayExpression array} raw type,
         * but relies on the elements of the array being of the type derived from the user code.
         *
         * <p>One may think of it as a more specific version of {@link #TYPE}.</p>
         */
        TYPE_ARGUMENT,
        /**
         * The API relies on the array being non-empty.
         * For example, {@link com.mongodb.client.model.expressions.ArrayExpression#first()}.
         */
        NON_EMPTY,
        /**
         * The API relies on the element identified by index, name, etc., being present in the
         * {@linkplain com.mongodb.client.model.expressions.DocumentExpression document} involved.
         * For example, {@link com.mongodb.client.model.expressions.DocumentExpression#getField(String)}.
         */
        PRESENT,
    }
}
