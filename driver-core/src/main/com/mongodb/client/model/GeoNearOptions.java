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
import org.bson.conversions.Bson;

/**
 * The options for a {@link Aggregates#geoNear} pipeline stage.
 *
 * @mongodb.driver.manual reference/operator/aggregation/unwind/ $geoNear
 * @since 4.8
 */
public interface GeoNearOptions extends Bson {
    /**
     * Returns {@link GeoNearOptions} that represents server defaults.
     *
     * @return {@link GeoNearOptions} that represents server defaults.
     */
    static GeoNearOptions geoNearOptions() {
        return GeoNearConstructibleBson.EMPTY_IMMUTABLE;
    }

    /**
     * @param distanceMultiplier The factor to multiply all distances returned by the query.
     * @return a new {@link GeoNearOptions} with the provided option set
     * @since 4.8
     */
    GeoNearOptions distanceMultiplier(Number distanceMultiplier);

    /**
     * This specifies the output field that identifies the location used to calculate the distance.
     * This option is useful when a location field contains multiple locations.
     * To specify a field within an embedded document, use dot notation.
     *
     * @param includeLocs the output field
     * @return a new {@link GeoNearOptions} with the provided option set
     * @since 4.8
     */
    GeoNearOptions includeLocs(String includeLocs);

    /**
     * Specify the geospatial indexed field to use when calculating the distance.
     *
     * @param key the geospatial indexed field.
     * @return a new {@link GeoNearOptions} with the provided option set
     * @since 4.8
     */
    GeoNearOptions key(String key);

    /**
     * The minimum distance from the center point that the documents can be.
     * MongoDB limits the results to those documents that fall outside the specified distance from the center point.
     *
     * @param minDistance the distance in meters for GeoJSON data.
     * @return a new {@link GeoNearOptions} with the provided option set
     * @since 4.8
     */
    GeoNearOptions minDistance(Number minDistance);

    /**
     * The maximum distance from the center point that the documents can be.
     * MongoDB limits the results to those documents that fall within the specified distance from the center point.
     *
     * @param maxDistance the distance in meters for GeoJSON data.
     * @return a new {@link GeoNearOptions} with the provided option set
     * @since 4.8
     */
    GeoNearOptions maxDistance(Number maxDistance);

    /**
     * Limits the results to the documents that match the query.
     * The query syntax is the usual MongoDB read operation query syntax.
     *
     * @param query the query
     * @return a new {@link GeoNearOptions} with the provided option set
     * @since 4.8
     */
    GeoNearOptions query(Document query);

    /**
     * Determines how MongoDB calculates the distance between two points.
     * By default, when this option is not provided, MongoDB uses $near semantics:
     * spherical geometry for 2dsphere indexes and planar geometry for 2d indexes.
     * When provided, MongoDB uses $nearSphere semantics and calculates distances
     * using spherical geometry.
     *
     * @return a new {@link GeoNearOptions} with the provided option set
     * @since 4.8
     */
    GeoNearOptions spherical();
}
