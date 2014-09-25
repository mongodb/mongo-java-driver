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

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * A model describing a bulk write, which can include a combination of inserts, updates, and deletes.  By default the writes are ordered.
 *
 * @param <T> the document type for the documents to be added to a collection
 * @since 3.0
 */
public final class BulkWriteModel<T> {
    private final List<? extends WriteModel<? extends T>> requests;
    private BulkWriteOptions options;

    /**
     * Construct a new instance with the given list of write models.
     *
     * @param requests a non-null, non-empty list of write models
     */
    public BulkWriteModel(final List<? extends WriteModel<? extends T>> requests) {
       this(requests, new BulkWriteOptions());
    }

    /**
     * Construct a new instance with the given list of write models.
     *
     * @param requests a non-null, non-empty list of write models
     * @param options the non-null bulk write options
     */
    public BulkWriteModel(final List<? extends WriteModel<? extends T>> requests, final BulkWriteOptions options) {
        this.requests = notNull("requests", requests);
        isTrueArgument("requests list is not empty", !requests.isEmpty());
        this.options = notNull("options", options);
    }

    /**
     * Gets the write models.
     *
     * @return a non-null, non-empty list of write models
     */
    public List<? extends WriteModel<? extends T>> getRequests() {
        return requests;
    }

    /**
     * Gets the options to apply
     *
     * @return the options
     */
    public BulkWriteOptions getOptions() {
        return options;
    }
}
