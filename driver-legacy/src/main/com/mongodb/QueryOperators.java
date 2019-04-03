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

package com.mongodb;

/**
 * MongoDB keywords for various query operations.
 *
 * @mongodb.driver.manual reference/operator/query/ Query Operators
 */
public class QueryOperators {
    public static final String OR = "$or";
    public static final String AND = "$and";

    public static final String GT = "$gt";
    public static final String GTE = "$gte";
    public static final String LT = "$lt";
    public static final String LTE = "$lte";

    public static final String NE = "$ne";
    public static final String IN = "$in";
    public static final String NIN = "$nin";
    public static final String MOD = "$mod";
    public static final String ALL = "$all";
    public static final String SIZE = "$size";
    public static final String EXISTS = "$exists";
    public static final String ELEM_MATCH = "$elemMatch";

    // (to be implemented in QueryBuilder)
    public static final String WHERE = "$where";
    public static final String NOR = "$nor";
    public static final String TYPE = "$type";
    public static final String NOT = "$not";

    // geo operators
    public static final String WITHIN = "$within";
    public static final String NEAR = "$near";
    public static final String NEAR_SPHERE = "$nearSphere";
    public static final String BOX = "$box";
    public static final String CENTER = "$center";
    public static final String POLYGON = "$polygon";
    public static final String CENTER_SPHERE = "$centerSphere";
    // (to be implemented in QueryBuilder)
    public static final String MAX_DISTANCE = "$maxDistance";
    public static final String UNIQUE_DOCS = "$uniqueDocs";

    // text operators
    public static final String TEXT = "$text";
    public static final String SEARCH = "$search";
    public static final String LANGUAGE = "$language";

    private QueryOperators() {
    }
}
