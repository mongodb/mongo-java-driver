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
package com.mongodb.client.model.fill;

import com.mongodb.annotations.Evolving;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Sorts;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

/**
 * Represents optional fields of the {@code $fill} pipeline stage of an aggregation pipeline.
 *
 * @see Aggregates#fill(FillOptions, Iterable)
 * @see Aggregates#fill(FillOptions, FillOutputField, FillOutputField...)
 * @mongodb.server.release 5.3
 * @since 4.7
 */
@Evolving
public interface FillOptions extends Bson {
    /**
     * Creates a new {@link FillOptions} with the specified partitioning.
     * Overrides {@link #partitionByFields(Iterable)}.
     *
     * @param expression The expression specifying how to partition the data.
     * The syntax is the same as the syntax for {@code id} in {@link Aggregates#group(Object, List)}.
     * @param <TExpression> The type of the {@code expression} expression.
     * @return A new {@link FillOptions}.
     */
    <TExpression> FillOptions partitionBy(TExpression expression);

    /**
     * Creates a new {@link FillOptions} with the specified partitioning.
     * Overrides {@link #partitionBy(Object)}.
     *
     * @param fields The fields to partition by.
     * @return A new {@link FillOptions}.
     * @mongodb.driver.manual core/document/#dot-notation
     */
    default FillOptions partitionByFields(@Nullable final String... fields) {
        return partitionByFields(fields == null ? emptyList() : asList(fields));
    }

    /**
     * Creates a new {@link FillOptions} with the specified partitioning.
     * Overrides {@link #partitionBy(Object)}.
     *
     * @param fields The fields to partition by.
     * @return A new {@link FillOptions}.
     * @mongodb.driver.manual core/document/#dot-notation
     */
    FillOptions partitionByFields(Iterable<String> fields);

    /**
     * Creates a new {@link FillOptions} with the specified sorting.
     *
     * @param sortBy The sort specification, which may be constructed via {@link Sorts}.
     * @return A new {@link FillOptions}.
     */
    FillOptions sortBy(Bson sortBy);

    /**
     * Creates a new {@link FillOptions} with the specified option in situations when there is no builder method
     * that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link FillOptions} objects,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  FillOptions options1 = FillOptions.fillOptions().partitionByFields("fieldName");
     *  FillOptions options2 = FillOptions.fillOptions().option("partitionByFields", Collections.singleton("fieldName"));
     * }</pre>
     *
     * @param name The option name.
     * @param value The option value.
     * @return A new {@link FillOptions}.
     */
    FillOptions option(String name, Object value);

    /**
     * Returns {@link FillOptions} that represents server defaults.
     *
     * @return {@link FillOptions} that represents server defaults.
     */
    static FillOptions fillOptions() {
        return FillConstructibleBson.EMPTY_IMMUTABLE;
    }
}
