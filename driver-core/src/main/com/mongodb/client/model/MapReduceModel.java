/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A model describing a map-reduce operation.
 *
 * @since 3.0
 * @mongodb.driver.manual reference/command/mapReduce/ map-reduce Command
 * @mongodb.driver.manual core/map-reduce/ map-reduce Overview
 */
public class MapReduceModel implements ExplainableModel {
    private final String mapFunction;
    private final String reduceFunction;
    private final MapReduceOptions options;

    /**
     * Constructs a new instance.
     *
     * @param mapFunction A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    public MapReduceModel(final String mapFunction, final String reduceFunction) {
        this(mapFunction, reduceFunction, new MapReduceOptions());
    }

    /**
     * Constructs a new instance.
     *
     * @param mapFunction A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param mapReduceOptions The specific options for the map-reduce command.
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    public MapReduceModel(final String mapFunction, final String reduceFunction, final MapReduceOptions mapReduceOptions) {
        this.mapFunction = notNull("mapFunction", mapFunction);
        this.reduceFunction = notNull("reduceFunction", reduceFunction);
        this.options = notNull("mapReduceOptions", mapReduceOptions);
    }

    /**
     * Gets the JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     *
     * @return the JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     */
    public String getMapFunction() {
        return mapFunction;
    }

    /**
     * Gets the JavaScript function that "reduces" to a single object all the values associated with a particular key.
     *
     * @return the JavaScript function that "reduces" to a single object all the values associated with a particular key.
     */
    public String getReduceFunction() {
        return reduceFunction;
    }

    /**
     * Gets the options to apply.
     *
     * @return the options
     */
    public MapReduceOptions getOptions() {
        return options;
    }
}
