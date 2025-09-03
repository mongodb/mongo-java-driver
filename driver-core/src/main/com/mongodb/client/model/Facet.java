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

import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;

/**
 * Defines a Facet for use in $facet pipeline stages.
 *
 * @mongodb.driver.manual reference/operator/aggregation/facet/ $facet
 * @mongodb.server.release 3.4
 * @since 3.4
 */
public class Facet {
    private final String name;
    private final List<? extends Bson> pipeline;

    /**
     * @param name     the name of this facet
     * @param pipeline the facet definition pipeline
     */
    public Facet(final String name, final List<? extends Bson> pipeline) {
        this.name = name;
        this.pipeline = pipeline;
    }

    /**
     * @param name     the name of this facet
     * @param pipeline the facet definition pipeline
     */
    public Facet(final String name, final Bson... pipeline) {
        this(name, asList(pipeline));
    }

    /**
     * @return the facet name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the pipeline definition
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

        Facet facet = (Facet) o;

        if (!Objects.equals(name, facet.name)) {
            return false;
        }
        return Objects.equals(pipeline, facet.pipeline);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (pipeline != null ? pipeline.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Facet{"
                + "name='" + name + '\''
                + ", pipeline=" + pipeline
                + '}';
    }
}
