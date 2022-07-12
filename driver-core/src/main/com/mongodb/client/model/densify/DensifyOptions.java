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
package com.mongodb.client.model.densify;

import com.mongodb.annotations.Evolving;
import com.mongodb.client.model.Aggregates;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

/**
 * Represents optional fields of the {@code $densify} pipeline stage of an aggregation pipeline.
 *
 * @see Aggregates#densify(String, DensifyRange, DensifyOptions)
 * @see Aggregates#densify(String, DensifyRange)
 * @mongodb.server.release 5.1
 * @since 4.7
 */
@Evolving
public interface DensifyOptions extends Bson {
    /**
     * Creates a new {@link DensifyOptions} with the specified {@code fields} to partition by.
     *
     * @param fields The fields to partition by.
     * If no fields are specified, then the whole sequence is considered to be a single partition.
     * @return A new {@link DensifyOptions}.
     * @mongodb.driver.manual core/document/#dot-notation Dot notation
     */
    default DensifyOptions partitionByFields(@Nullable final String... fields) {
        return partitionByFields(fields == null ? emptyList() : asList(fields));
    }

    /**
     * Creates a new {@link DensifyOptions} with the specified {@code fields} to partition by.
     *
     * @param fields The fields to partition by.
     * If no fields are specified, then the whole sequence is considered to be a single partition.
     * @return A new {@link DensifyOptions}.
     * @mongodb.driver.manual core/document/#dot-notation Dot notation
     */
    DensifyOptions partitionByFields(Iterable<String> fields);

    /**
     * Creates a new {@link DensifyOptions} with the specified option in situations when there is no builder method
     * that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link DensifyOptions} objects,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  DensifyOptions options1 = DensifyOptions.densifyOptions()
     *          .partitionByFields("fieldName");
     *  DensifyOptions options2 = DensifyOptions.densifyOptions()
     *          .option("partitionByFields", Collections.singleton("fieldName"));
     * }</pre>
     *
     * @param name The option name.
     * @param value The option value.
     * @return A new {@link DensifyOptions}.
     */
    DensifyOptions option(String name, Object value);

    /**
     * Returns {@link DensifyOptions} that represents server defaults.
     *
     * @return {@link DensifyOptions} that represents server defaults.
     */
    static DensifyOptions densifyOptions() {
        return DensifyConstructibleBson.EMPTY_IMMUTABLE;
    }
}
