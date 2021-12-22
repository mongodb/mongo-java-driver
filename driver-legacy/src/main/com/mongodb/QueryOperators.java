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
    /**
     * OR
     */
    public static final String OR = "$or";
    /**
     * AND
     */
    public static final String AND = "$and";

    /**
     * GT
     */
    public static final String GT = "$gt";
    /**
     * GTE
     */
    public static final String GTE = "$gte";
    /**
     * LT
     */
    public static final String LT = "$lt";
    /**
     * LTE
     */
    public static final String LTE = "$lte";

    /**
     * NE
     */
    public static final String NE = "$ne";
    /**
     * IN
     */
    public static final String IN = "$in";
    /**
     * NIN
     */
    public static final String NIN = "$nin";
    /**
     * MOD
     */
    public static final String MOD = "$mod";
    /**
     * ALL
     */
    public static final String ALL = "$all";
    /**
     * SIZE
     */
    public static final String SIZE = "$size";
    /**
     * EXISTS
     */
    public static final String EXISTS = "$exists";
    /**
     * ELEM_MATCH
     */
    public static final String ELEM_MATCH = "$elemMatch";

    // (to be implemented in QueryBuilder)

    /**
     * WHERE
     */
    public static final String WHERE = "$where";
    /**
     * NOR
     */
    public static final String NOR = "$nor";
    /**
     * TYPE
     */
    public static final String TYPE = "$type";
    /**
     * NOT
     */
    public static final String NOT = "$not";

    // geo operators

    /**
     * WITHIN
     */
    public static final String WITHIN = "$within";
    /**
     * NEAR
     */
    public static final String NEAR = "$near";
    /**
     * NEAR_SPHERE
     */
    public static final String NEAR_SPHERE = "$nearSphere";
    /**
     * BOX
     */
    public static final String BOX = "$box";
    /**
     * CENTER
     */
    public static final String CENTER = "$center";
    /**
     * POLYGON
     */
    public static final String POLYGON = "$polygon";
    /**
     * CENTER_SPHERE
     */
    public static final String CENTER_SPHERE = "$centerSphere";

    // (to be implemented in QueryBuilder)

    /**
     * MAX_DISTANCE
     */
    public static final String MAX_DISTANCE = "$maxDistance";
    /**
     * UNIQUE_DOCS
     */
    public static final String UNIQUE_DOCS = "$uniqueDocs";

    // text operators

    /**
     * TEXT
     */
    public static final String TEXT = "$text";
    /**
     * SEARCH
     */
    public static final String SEARCH = "$search";
    /**
     * LANGUAGE
     */
    public static final String LANGUAGE = "$language";

    private QueryOperators() {
    }
}
