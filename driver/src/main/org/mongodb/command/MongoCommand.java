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

import org.mongodb.ConvertibleToDocument;
import org.mongodb.Document;
import org.mongodb.ReadPreference;
import org.mongodb.operation.MongoQuery;
import org.mongodb.operation.QueryOption;

import java.util.EnumSet;

public class MongoCommand extends MongoQuery implements ConvertibleToDocument {
    private final Document command;

    public MongoCommand(final Document commandDocument) {
        this.command = commandDocument;
        batchSize = -1;
    }

    @Override
    public MongoCommand readPreference(final ReadPreference readPreference) {
        super.readPreference(readPreference);
        return this;
    }


    @Override
    public MongoCommand addOptions(final EnumSet<QueryOption> options) {
        super.addOptions(options);
        return this;
    }

    /**
     * Commands always have a batch size of -1.
     *
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

    @Override
    public Document toDocument() {
        return command;
    }
}
