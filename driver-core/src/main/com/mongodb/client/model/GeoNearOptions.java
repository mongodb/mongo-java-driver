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

import org.bson.Document;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The options for a {@link Aggregates#geoNear} pipeline stage.
 *
 * @mongodb.driver.manual reference/operator/aggregation/unwind/ $geoNear
 * @since 4.8
 */
public final class GeoNearOptions {

    private final Map<String, Object> options;

    public static GeoNearOptions geoNearOptions() {
        return new GeoNearOptions(Collections.unmodifiableMap(new HashMap<>()));
    }

    private GeoNearOptions(final Map<String, Object> options) {
        this.options = options;
    }

    private GeoNearOptions setOption(final String key, final Object value) {
        Map<String, Object> options = new HashMap<>(this.options);
        options.put(key, value);
        return new GeoNearOptions(options);
    }

    void appendToDocument(Document document) {
        for (Map.Entry<String, Object> e : this.options.entrySet()) {
            document.append(e.getKey(), e.getValue());
        }
    }

    /**
     * @param distanceMultiplier The factor to multiply all distances returned by the query.
     * @return the option
     * @since 4.8
     */
    public GeoNearOptions distanceMultiplier(final Number distanceMultiplier) {
        return setOption("distanceMultiplier", distanceMultiplier);
    }

    /**
     * This specifies the output field that identifies the location used to calculate the distance.
     * This option is useful when a location field contains multiple locations.
     * To specify a field within an embedded document, use dot notation.
     *
     * @param includeLocs the output field
     * @return the option
     * @since 4.8
     */
    public GeoNearOptions includeLocs(final String includeLocs) {
        return setOption("includeLocs", includeLocs);
    }

    /**
     * Specify the geospatial indexed field to use when calculating the distance.
     *
     * @param key the geospatial indexed field.
     * @return the option
     * @since 4.8
     */
    public GeoNearOptions key(final String key) {
        return setOption("key", key);
    }

    /**
     * The minimum distance from the center point that the documents can be.
     * MongoDB limits the results to those documents that fall outside the specified distance from the center point.
     *
     * @param minDistance the distance in meters for GeoJSON data.
     * @return the option
     * @since 4.8
     */
    public GeoNearOptions minDistance(final Number minDistance) {
        return setOption("minDistance", minDistance);
    }

    /**
     * The maximum distance from the center point that the documents can be.
     * MongoDB limits the results to those documents that fall within the specified distance from the center point.
     *
     * @param maxDistance the distance in meters for GeoJSON data.
     * @return the option
     * @since 4.8
     */
    public GeoNearOptions maxDistance(final Number maxDistance) {
        return setOption("maxDistance", maxDistance);
    }

    /**
     * Limits the results to the documents that match the query.
     * The query syntax is the usual MongoDB read operation query syntax.
     *
     * @param query the query
     * @return the option
     * @since 4.8
     */
    public GeoNearOptions query(final Document query) {
        return setOption("query", query);
    }

    /**
     * Determines how MongoDB calculates the distance between two points.
     * By default, when this option is not provided, MongoDB uses $near semantics:
     * spherical geometry for 2dsphere indexes and planar geometry for 2d indexes.
     * When provided, MongoDB uses $nearSphere semantics and calculates distances
     * using spherical geometry.
     *
     * @return the option
     * @since 4.8
     */
    public GeoNearOptions spherical() {
        return setOption("spherical", true);
    }
}
