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

package com.mongodb.client.model;

/**
 * The options to apply to an operation that inserts a single document into a collection.
 *
 * @since 3.2
 * @mongodb.server.release 3.2
 * @mongodb.driver.manual tutorial/insert-documents/ Insert Tutorial
 * @mongodb.driver.manual reference/command/insert/ Insert Command
 */
public final class InsertOneOptions {
    private Boolean bypassDocumentValidation;

    /**
     * Gets the the bypass document level validation flag
     *
     * @return the bypass document level validation flag
     */
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * Sets the bypass document level validation flag.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     */
    public InsertOneOptions bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public String toString() {
        return "InsertOneOptions{"
                + "bypassDocumentValidation=" + bypassDocumentValidation
                + '}';
    }
}
