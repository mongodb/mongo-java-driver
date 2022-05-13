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
 * WITHOUNumber WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.client.model.search;

import com.mongodb.annotations.Evolving;

/**
 * A base for a {@link NumberRangeSearchOperator} which allows creating instances of this operator.
 * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
 *
 * @see SearchOperator#numberRange(FieldSearchPath)
 * @see SearchOperator#numberRange(Iterable)
 * @since 4.7
 */
@Evolving
public interface NumberRangeSearchOperatorBase extends RangeSearchOperatorBase<Number> {
    @Override
    NumberRangeSearchOperator gt(Number l);

    @Override
    NumberRangeSearchOperator lt(Number u);

    @Override
    NumberRangeSearchOperator gte(Number l);

    @Override
    NumberRangeSearchOperator lte(Number u);

    @Override
    NumberRangeSearchOperator gtLt(Number l, Number u);

    @Override
    NumberRangeSearchOperator gteLte(Number l, Number u);

    @Override
    NumberRangeSearchOperator gtLte(Number l, Number u);

    @Override
    NumberRangeSearchOperator gteLt(Number l, Number u);
}
