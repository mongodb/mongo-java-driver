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

/**
 * Query building API for MongoDB Atlas full-text search.
 *
 * @see com.mongodb.client.model.Aggregates#search(SearchOperator, SearchOptions)
 * @see com.mongodb.client.model.Aggregates#search(SearchCollector, SearchOptions)
 * @mongodb.atlas.manual atlas-search/ Atlas search
 * @mongodb.atlas.manual atlas-search/query-syntax/ Atlas search aggregation pipeline stages
 * @since 4.6
 */
@NonNullApi
package com.mongodb.client.model.search;

import com.mongodb.lang.NonNullApi;
