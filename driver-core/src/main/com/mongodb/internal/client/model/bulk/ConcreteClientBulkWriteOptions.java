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
package com.mongodb.internal.client.model.bulk;

import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.lang.Nullable;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class ConcreteClientBulkWriteOptions implements ClientBulkWriteOptions {
    // BULK-TODO Introduce EMPTY similar to ConcreteClientUpdateOptions.EMPTY.
    private static final Boolean CLIENT_DEFAULT_ORDERED = true;
    private static final Boolean CLIENT_DEFAULT_VERBOSE_RESULTS = false;

    @Nullable
    private Boolean ordered;
    @Nullable
    private Boolean bypassDocumentValidation;
    @Nullable
    private Bson let;
    @Nullable
    private BsonValue comment;
    @Nullable
    private Boolean verboseResults;

    public ConcreteClientBulkWriteOptions() {
    }

    @Override
    public ClientBulkWriteOptions ordered(@Nullable final Boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    @Override
    public ClientBulkWriteOptions bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public ClientBulkWriteOptions let(@Nullable final Bson let) {
        this.let = let;
        return this;
    }

    @Override
    public ClientBulkWriteOptions comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public ClientBulkWriteOptions verboseResults(@Nullable final Boolean verboseResults) {
        this.verboseResults = verboseResults;
        return this;
    }

    @Override
    public String toString() {
        return "ClientBulkWriteOptions{"
                + "ordered=" + ordered
                + ", bypassDocumentValidation=" + bypassDocumentValidation
                + ", let=" + let
                + ", comment=" + comment
                + ", verboseResults=" + verboseResults
                + '}';
    }
}
