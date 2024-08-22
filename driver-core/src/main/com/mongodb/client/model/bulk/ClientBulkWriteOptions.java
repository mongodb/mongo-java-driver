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
package com.mongodb.client.model.bulk;

import com.mongodb.annotations.Sealed;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.bulk.ClientBulkWriteResult;
import com.mongodb.internal.client.model.bulk.ConcreteClientBulkWriteOptions;
import com.mongodb.lang.Nullable;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

/**
 * The options to apply when executing a client-level bulk write operation.
 *
 * @since 5.3
 */
@Sealed
public interface ClientBulkWriteOptions {
    /**
     * Creates the default options.
     *
     * @return The default options.
     */
    static ClientBulkWriteOptions clientBulkWriteOptions() {
        return new ConcreteClientBulkWriteOptions();
    }

    /**
     * Enables or disables ordered execution of {@linkplain ClientWriteModel individual write operations}.
     * In an ordered execution a failure of an individual operation prevents the rest of them
     * from being executed.
     * In an unordered execution failures of individual operations do not prevent the rest of them
     * from being executed.
     *
     * @param ordered The ordered flag. If {@code null}, the client defaults to {@code true}.
     * @return {@code this}.
     */
    ClientBulkWriteOptions ordered(@Nullable Boolean ordered);

    /**
     * Disables or enables checking against document validation rules, a.k.a., schema validation.
     *
     * @param bypassDocumentValidation The flag specifying whether to bypass the document validation rules.
     * {@code null} represents the server default.
     * @return {@code this}.
     */
    ClientBulkWriteOptions bypassDocumentValidation(@Nullable Boolean bypassDocumentValidation);

    /**
     * Sets variables that can be referenced from {@linkplain ClientWriteModel individual write operations}
     * with the {@code "$$"} syntax, which in turn requires using {@link Filters#expr(Object)} when specifying filters.
     * Values must be constants or expressions that do not reference fields.
     *
     * @param let The variables. {@code null} represents the server default.
     * @return {@code this}.
     * @mongodb.driver.manual reference/aggregation-variables/ Variables in Aggregation Expressions
     */
    ClientBulkWriteOptions let(@Nullable Bson let);

    /**
     * Sets the comment to attach to the {@code bulkWrite} administration command.
     *
     * @param comment The comment. {@code null} represents the server default.
     * @return {@code this}.
     */
    ClientBulkWriteOptions comment(@Nullable BsonValue comment);

    /**
     * Enables or disables requesting {@linkplain ClientBulkWriteResult#getVerbose() verbose results}.
     *
     * @param verboseResults The flag specifying whether to request verbose results.
     * If {@code null}, the client defaults to {@code false}.
     * This value corresponds inversely to the {@code errorsOnly} field of the {@code bulkWrite} administration command.
     * @return {@code this}.
     */
    ClientBulkWriteOptions verboseResults(@Nullable Boolean verboseResults);
}
