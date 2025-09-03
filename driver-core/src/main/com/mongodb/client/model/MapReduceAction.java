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



/**
 * The map reduce to collection actions.
 *
 * <p>These actions are only available when passing out a collection that already exists. This option is not available on secondary members
 * of replica sets.  The Enum values dictate what to do with the output collection if it already exists when the map reduce is run.</p>
 *
 * @since 3.0
 * @mongodb.driver.manual reference/command/mapReduce/ mapReduce Command
 * @mongodb.driver.manual core/map-reduce/ mapReduce Overview
 * @deprecated Superseded by aggregate
 */
@Deprecated
public enum MapReduceAction {
    /**
     * Replace the contents of the {@code collectionName} if the collection with the {@code collectionName} exists.
     */
    REPLACE("replace"),

    /**
     * Merge the new result with the existing result if the output collection already exists. If an existing document has the same key
     * as the new result, overwrite that existing document.
     */
    MERGE("merge"),

    /**
     * Merge the new result with the existing result if the output collection already exists. If an existing document has the same key
     * as the new result, apply the reduce function to both the new and the existing documents and overwrite the existing document with
     * the result.
     */
    REDUCE("reduce");

    private final String value;

    MapReduceAction(final String value) {
        this.value = value;
    }

    /**
     * @return the String representation of this Action that the MongoDB server understands
     */
    public String getValue() {
        return value;
    }
}
