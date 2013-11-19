/*
 * Copyright (c) 2008 - 2013 MongoDB, Inc. <http://10gen.com>
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

package org.mongodb.operation;

import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MapReduceStatistics;

/**
 * An instance of this class will contain the statistics returned from running a map-reduce and putting the results into a Collection.
 *
 * @since 3.0
 */
public class MapReduceIntoCollectionStatistics implements MapReduceStatistics {
    private final CommandResult commandResult;

    /**
     * Create a new class that implements MapReduceStatistics to return all the stats from running a map-reduce that does not return the
     * results inline, i.e. results are saved in a collection.
     *
     * @param commandResult the result of the map-reduce operation
     */
    public MapReduceIntoCollectionStatistics(final CommandResult commandResult) {
        this.commandResult = commandResult;
    }

    @Override
    public int getInputCount() {
        return ((Document) commandResult.getResponse().get("counts")).getInteger("input");
    }

    @Override
    public int getOutputCount() {
        return ((Document) commandResult.getResponse().get("counts")).getInteger("output");
    }

    @Override
    public int getEmitCount() {
        return ((Document) commandResult.getResponse().get("counts")).getInteger("emit");
    }

    @Override
    public int getDuration() {
        return commandResult.getResponse().getInteger("timeMillis");
    }
}
