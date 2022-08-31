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

/**
 * Optional fields for the {@link Aggregates#geoNear} pipeline stage.
 */
public final class GeoNearOption {
    private final String key;
    private final Object value;

    String getKey() {
        return key;
    }
    Object getValue() {
        return value;
    }

    private GeoNearOption(final String key, final Object value) {
        this.key = key;
        this.value = value;
    }

    /**
     * @param distanceMultiplier The factor to multiply all distances returned by the query.
     * @return the option
     */
    public static GeoNearOption distanceMultiplier(final Number distanceMultiplier) {
        return new GeoNearOption("distanceMultiplier", distanceMultiplier);
    }

    /**
     * This specifies the output field that identifies the location used to calculate the distance.
     * This option is useful when a location field contains multiple locations.
     * To specify a field within an embedded document, use dot notation.
     *
     * @param includeLocs the output field
     * @return the option
     */
    public static GeoNearOption includeLocs(final String includeLocs) {
        return new GeoNearOption("includeLocs", includeLocs);
    }

    /**
     * Specify the geospatial indexed field to use when calculating the distance.
     *
     * @param key the geospatial indexed field.
     * @return the option
     */
    public static GeoNearOption key(final String key) {
        return new GeoNearOption("key", key);
    }

    /**
     * The minimum distance from the center point that the documents can be.
     * MongoDB limits the results to those documents that fall outside the specified distance from the center point.
     *
     * @param minDistance the distance in meters for GeoJSON data.
     * @return the option
     */
    public static GeoNearOption minDistance(final Number minDistance) {
        return new GeoNearOption("minDistance", minDistance);
    }

    /**
     * The maximum distance from the center point that the documents can be.
     * MongoDB limits the results to those documents that fall within the specified distance from the center point.
     *
     * @param maxDistance the distance in meters for GeoJSON data.
     * @return the option
     */
    public static GeoNearOption maxDistance(final Number maxDistance) {
        return new GeoNearOption("maxDistance", maxDistance);
    }

    /**
     * Limits the results to the documents that match the query.
     * The query syntax is the usual MongoDB read operation query syntax.
     *
     * @param query the query
     * @return the option
     */
    public static GeoNearOption query(final Document query) {
        return new GeoNearOption("query", query);
    }

    /**
     * Determines how MongoDB calculates the distance between two points.
     * By default, when this option is not provided, MongoDB uses $near semantics:
     * spherical geometry for 2dsphere indexes and planar geometry for 2d indexes.
     * When provided, MongoDB uses $nearSphere semantics and calculates distances
     * using spherical geometry.
     *
     * @return the option
     */
    public static GeoNearOption spherical() {
        return new GeoNearOption("spherical", true);
    }
}
