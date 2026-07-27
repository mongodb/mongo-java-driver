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

import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

/**
 * A named aggregation pipeline used as an input to a fusion pipeline stage, e.g.,
 * {@link Aggregates#scoreFusion(List, ScoreNormalization, ScoreFusionOptions) $scoreFusion}.
 * The name uniquely identifies the pipeline within the stage and may be referred to
 * by other parts of the stage, e.g., as the {@code "$$name"} variable in a
 * {@linkplain ScoreFusionCombination#expression(Bson) combination expression}.
 *
 * @since 5.10
 * @mongodb.server.release 8.2
 */
public final class FusionPipeline {
    private final String name;
    private final List<Bson> pipeline;

    /**
     * Creates a new {@link FusionPipeline}.
     *
     * @param name The non-empty pipeline name, unique within the containing stage.
     * @param pipeline The non-empty pipeline.
     * @return The requested {@link FusionPipeline}.
     */
    public static FusionPipeline of(final String name, final List<? extends Bson> pipeline) {
        return new FusionPipeline(name, pipeline);
    }

    /**
     * Creates a new {@link FusionPipeline}.
     *
     * @param name The non-empty pipeline name, unique within the containing stage.
     * @param pipeline The non-empty pipeline.
     * @return The requested {@link FusionPipeline}.
     */
    public static FusionPipeline of(final String name, final Bson... pipeline) {
        return new FusionPipeline(name, asList(pipeline));
    }

    private FusionPipeline(final String name, final List<? extends Bson> pipeline) {
        notNull("name", name);
        isTrueArgument("name must not be empty", !name.trim().isEmpty());
        notNull("pipeline", pipeline);
        isTrueArgument("pipeline must not be empty", !pipeline.isEmpty());
        for (Bson stage : pipeline) {
            notNull("stage", stage);
        }
        this.name = name;
        this.pipeline = unmodifiableList(new ArrayList<Bson>(pipeline));
    }

    /**
     * @return the pipeline name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the pipeline
     */
    public List<? extends Bson> getPipeline() {
        return pipeline;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FusionPipeline that = (FusionPipeline) o;
        return name.equals(that.name) && pipeline.equals(that.pipeline);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, pipeline);
    }

    @Override
    public String toString() {
        return "FusionPipeline{"
                + "name='" + name + '\''
                + ", pipeline=" + pipeline
                + '}';
    }
}
