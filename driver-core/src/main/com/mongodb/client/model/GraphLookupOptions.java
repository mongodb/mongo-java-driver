/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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

/**
 * The options for a graphLookup aggregation pipeline stage
 *
 * @mongodb.driver.manual reference/operator/aggregation/graphLookup/ graphLookup
 * @mongodb.server.release 3.4
 * @since 3.4
 */
public final class GraphLookupOptions {
    private Integer maxDepth;
    private String depthField;
    private Bson restrictSearchWithMatch;

    /**
     * The name of the field in which to store the depth value
     *
     * @param field the field name
     * @return this
     */
    public GraphLookupOptions depthField(final String field) {
        depthField = field;
        return this;
    }

    /**
     * @return the field name
     */
    public String getDepthField() {
        return depthField;
    }

    /**
     * Specifies a maximum recursive depth for the $graphLookup.  This number must be non-negative.
     *
     * @param max the maximum depth
     * @return this
     */
    public GraphLookupOptions maxDepth(final Integer max) {
        maxDepth = max;
        return this;
    }

    /**
     * @return the maximum depth
     */
    public Integer getMaxDepth() {
        return maxDepth;
    }

    /**
     * A document specifying additional conditions for the recursive search
     *
     * @param filter the filter expression
     * @return this
     * @since 3.6
     */
    public GraphLookupOptions restrictSearchWithMatch(final Bson filter) {
        restrictSearchWithMatch = filter;
        return this;
    }

    /**
     * @return the filter expression
     * @since 3.6
     */
    public Bson getRestrictSearchWithMatch() {
        return restrictSearchWithMatch;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder()
            .append("GraphLookupOptions{");
        if (depthField != null) {
            stringBuilder.append("depthField='")
                         .append(depthField)
                         .append('\'');
            if (maxDepth != null) {
                stringBuilder.append(", ");
            }
        }
        if (maxDepth != null) {
            stringBuilder
                .append("maxDepth=")
                .append(maxDepth);
            if (restrictSearchWithMatch != null) {
                stringBuilder.append(", ");
            }
        }
        if (restrictSearchWithMatch != null) {
            stringBuilder
                .append("restrictSearchWithMatch=")
                .append(restrictSearchWithMatch);
        }

        return stringBuilder
            .append('}')
            .toString();
    }
}
