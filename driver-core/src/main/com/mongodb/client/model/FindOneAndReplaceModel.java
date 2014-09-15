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
 *
 * @param <T> The replacement type for the command. This can be of any type for which a {@code Codec} is registered
 * @since 3.0
 * @mongodb.driver.manual manual/reference/command/findAndModify/
 */
public class FindOneAndReplaceModel<T> {
    private final Object criteria;
    private final T replacement;
    private final FindOneAndReplaceOptions options;

    /**
     * Construct a new instance
     *
     * @param criteria    the query criteria. This can be of any type for which a {@code Codec} is registered.
     * @param replacement the replacement. This can be of any type for which a {@code Codec} is registered.
     * @mongodb.driver.manual manual/reference/command/findAndModify/
     */
    public FindOneAndReplaceModel(final Object criteria, final T replacement) {
        this(criteria, replacement, new FindOneAndReplaceOptions());
    }

    /**
     * Construct a new instance
     *
     * @param criteria    the query criteria. This can be of any type for which a {@code Codec} is registered.
     * @param replacement the replacement. This can be of any type for which a {@code Codec} is registered.
     * @param options     the options to apply
     * @mongodb.driver.manual manual/reference/command/findAndModify/
     */
    public FindOneAndReplaceModel(final Object criteria, final T replacement, final FindOneAndReplaceOptions options) {
        this.criteria = notNull("criteria", criteria);
        this.replacement = notNull("replacement", replacement);
        this.options = notNull("options", options);
    }

    /**
     * Gets the query criteria.
     *
     * @return the query criteria
     */
    public Object getCriteria() {
        return criteria;
    }

    /**
     * Gets the document which will replace the document matching the query filter.
     *
     * @return the replacement document
     */
    public T getReplacement() {
        return replacement;
    }

    /**
     * Gets the options to apply.
     *
     * @return the options
     */
    public FindOneAndReplaceOptions getOptions() {
        return options;
    }
}


