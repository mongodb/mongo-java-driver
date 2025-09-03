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

/**
 * A subset of documents within a partition in the {@link Aggregates#setWindowFields(Object, Bson, Iterable) $setWindowFields} pipeline stage
 * of an aggregation pipeline (see {@code partitionBy} in {@link Aggregates#setWindowFields(Object, Bson, Iterable)}).
 *
 * @see Windows
 * @since 4.3
 */
public interface Window extends Bson {
}
