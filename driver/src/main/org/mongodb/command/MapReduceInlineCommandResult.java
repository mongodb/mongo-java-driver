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


/**
 * A class that represents a result of map/reduce operation.
 *
 */
public class MapReduceInlineCommandResult<T> extends CommandResult {

    /**
     * Constructs a new instance of {@code MapReduceInlineCommandResult} from a {@code CommandResult}
     *
     * @param baseResult result of a command to use as a base
     */
    public MapReduceInlineCommandResult(final CommandResult baseResult) {
        super(baseResult);
    }

    /**
     * Extract the resulting documents of map/reduce operation if is inlined.
     *
     * @return collection of documents to be iterated through
     * @throws IllegalAccessError if resulting documents are not inlined.
     */
    @SuppressWarnings("unchecked")
    public Iterable<T> getResults() {
        return (Iterable<T>) getResponse().get("results");
    }


}
