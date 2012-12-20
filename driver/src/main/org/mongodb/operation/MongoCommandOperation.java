/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb.operation;

import org.mongodb.ReadPreference;


// TODO: Name is inconsistent with other operations.  Did this so as not to clash with MongoCommand
public class MongoCommandOperation extends MongoQuery {
    private final MongoCommand command;

    public MongoCommandOperation(final MongoCommand commandDocument) {
        this.command = commandDocument;
        batchSize = -1;
    }

    public MongoCommandOperation readPreference(final ReadPreference readPreference) {
        super.readPreference(readPreference);
        return this;
    }

    public MongoCommand getCommand() {
        return command;
    }

    /**
     * Commands always have a batch size of -1.
     * @return -1
     */
    public int getBatchSize() {
        return -1;
    }

    public int getSkip() {
        return 0;
    }

    public int getLimit() {
        return 0;
    }

}
