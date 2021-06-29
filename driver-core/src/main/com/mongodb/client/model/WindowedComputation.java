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

/**
 * The core part of the {@link Aggregates#setWindowFields(Object, Bson, List) $setWindowFields} pipeline stage of an aggregation pipeline.
 * A triple of a window function, a {@linkplain Window window} and a path to a field to be computed by the window function over the window.
 *
 * @see WindowedComputations
 * @since 4.3
 */
public interface WindowedComputation {
    /**
     * Returns a {@link BsonField} representation.
     *
     * @return A {@link BsonField} representation.
     */
    BsonField asBsonField();
}
