/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
 * A model describing a find operation (also commonly referred to as a query).
 *
 * @since 3.0
 * @mongodb.driver.manual manual/tutorial/query-documents/ Find
 * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 */
public final class FindModel implements ExplainableModel {
    private final FindOptions options;

    /**
     * Construct a new instance.
     */
    public FindModel() {
        this(new FindOptions());
    }

    /**
     * Construct an instance with the specified options
     *
     * @param options the options
     */
    public FindModel(final FindOptions options) {
        this.options = options;
    }

    /**
     * Construct a new instance by making a shallow copy of the given model.
     *
     * @param from model to copy
     */
    public FindModel(final FindModel from) {
        options = new FindOptions(from.options);
    }

    /**
     * Gets the options to apply.
     *
     * @return the options
     */
    public FindOptions getOptions() {
        return options;
    }
}

