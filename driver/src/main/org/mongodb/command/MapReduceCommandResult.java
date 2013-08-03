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

package org.mongodb.command;

import org.mongodb.CommandResult;
import org.mongodb.Document;

/**
 * A class that represents a result of map/reduce operation.
 */
public class MapReduceCommandResult extends CommandResult {

    /**
     * Constructs a new instance of {@code MapReduceCommandResult} from a {@code CommandResult}
     *
     * @param baseResult result of a command to use as a base
     */
    public MapReduceCommandResult(final CommandResult baseResult) {
        super(baseResult);
    }

    /**
     * Get a name of the collection that was used by map/reduce operation to write its output.
     *
     * @return a collection name
     */
    public String getCollectionName() {
        final Object result = getResponse().get("result");
        return (result instanceof Document)
                ? ((Document) result).getString("collection")
                : (String) result;
    }

    /**
     * Get a name of the database that was used by map/reduce operation to write its output.
     *
     * @return a database name
     */
    public String getDatabaseName() {
        final Object result = getResponse().get("result");
        return (result instanceof Document)
                ? ((Document) result).getString("db")
                : null;
    }
}
