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

package com.mongodb.client.model.search;

import com.mongodb.annotations.Beta;
import com.mongodb.annotations.Reason;
import com.mongodb.annotations.Sealed;
import com.mongodb.client.model.Aggregates;

/**
 * Represents optional fields of the {@code $vectorSearch} pipeline stage of an aggregation pipeline.
 * <p>
 * Configures approximate vector search for Atlas Vector Search to enable searches that may not return the exact closest vectors.
 *
 * @see Aggregates#vectorSearch(FieldSearchPath, Iterable, String, long, VectorSearchOptions)
 * @mongodb.atlas.manual atlas-vector-search/vector-search-stage/ $vectorSearch
 * @mongodb.server.release 6.0.11, 7.0.2 or greater.
 * @since 5.2
 */
@Sealed
@Beta(Reason.SERVER)
public interface ApproximateVectorSearchOptions extends VectorSearchOptions {
}
