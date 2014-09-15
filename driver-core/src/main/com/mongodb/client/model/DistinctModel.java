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

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A model describing a distinct operation.
 *
 * @since 3.0
 * @mongodb.driver.manual manual/reference/command/distinct/ Distinct
 */
public class DistinctModel implements ExplainableModel {
    private final String fieldName;
    private final DistinctOptions options;

    /**
     * Construct a new instance.
     *
     * @param fieldName the non-null field name to get the distinct values of
     */
    public DistinctModel(final String fieldName) {
        this(fieldName, new DistinctOptions());
    }

    /**
     * Construct a new instance.
     *
     * @param fieldName the non-null field name to get the distinct values of
     * @param options the options
     */
    public DistinctModel(final String fieldName, final DistinctOptions options) {
        this.fieldName = notNull("fieldName", fieldName);
        this.options = notNull("options", options);
    }

    /**
     * Gets the field name to get the distinct values of.
     *
     * @return the field name, which may not be null
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Gets the options to apply.
     *
     * @return the options
     */
    public DistinctOptions getOptions() {
        return options;
    }
}
