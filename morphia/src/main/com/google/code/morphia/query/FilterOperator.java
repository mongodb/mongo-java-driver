/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package com.google.code.morphia.query;

/**
 * @author Scott Hernandez
 */
public enum FilterOperator {
    NEAR("$near"),
    NEAR_SPHERE("$nearSphere"),
    WITHIN("$within"),
    WITHIN_CIRCLE("$center"),
    WITHIN_CIRCLE_SPHERE("$centerSphere"),
    WITHIN_BOX("$box"),
    EQUAL("$eq"),
    GREATER_THAN("$gt"),
    GREATER_THAN_OR_EQUAL("$gte"),
    LESS_THAN("$lt"),
    LESS_THAN_OR_EQUAL("$lte"),
    EXISTS("$exists"),
    TYPE("$type"),
    NOT("$not"),
    MOD("$mod"),
    SIZE("$size"),
    IN("$in"),
    NOT_IN("$nin"),
    ALL("$all"),
    ELEMENT_MATCH("$elemMatch"),
    NOT_EQUAL("$ne"),
    WHERE("$where");

    private final String value;

    private FilterOperator(final String val) {
        value = val;
    }

    private boolean equals(final String val) {
        return value.equals(val);
    }

    public String val() {
        return value;
    }

    public static FilterOperator fromString(final String val) {
        for (int i = 0; i < values().length; i++) {
            final FilterOperator fo = values()[i];
            if (fo.equals(val)) {
                return fo;
            }
        }
        return null;
    }
}