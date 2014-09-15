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

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A model describing an aggregation.
 *
 * @since 3.0
 * @mongodb.driver.manual manual/aggregation/ Aggregation
 * @mongodb.server.release 2.2
 */
public class AggregateModel implements ExplainableModel {
    private final List<?> pipeline;
    private final AggregateOptions options;

    /**
     * Construct an instance.
     *
     * @param pipeline the non-null aggregation pipeline
     */
    public AggregateModel(final List<?> pipeline) {
        this(pipeline, new AggregateOptions());
    }

    /**
     * Construct an instance.
     *
     * @param pipeline the non-null aggregation pipeline
     * @param options the options to apply
     */
    public AggregateModel(final List<?> pipeline, final AggregateOptions options) {
        this.pipeline = notNull("pipeline", pipeline);
        this.options = notNull("options", options);
    }

    /**
     * Gets the aggregation pipeline.
     *
     * @return the pipeline
     * @mongodb.driver.manual manual/core/aggregation-introduction/#aggregation-pipelines Aggregation Pipeline
     */
    public List<?> getPipeline() {
        return pipeline;
    }

    /**
     * Gets the options to apply.
     *
     * @return the options
     */
    public AggregateOptions getOptions() {
        return options;
    }
}
