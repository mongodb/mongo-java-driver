/*
 * Copyright (c) 2008 MongoDB, Inc.
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
import org.mongodb.operation.Query;
import org.mongodb.operation.QueryFlag;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

public class Command extends Query implements ConvertibleToDocument {
    private final Document command;

    public Command(final Document commandDocument) {
        this.command = commandDocument;
        getOptions().batchSize(-1);
    }

    @Override
    public Command readPreference(final ReadPreference readPreference) {
        super.readPreference(readPreference);
        return this;
    }


    @Override
    public Command addFlags(final EnumSet<QueryFlag> flags) {
        super.addFlags(flags);
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

    @Override
    public Query maxTime(final long maxTime, final TimeUnit timeUnit) {
        super.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        final Command command1 = (Command) o;

        if (!command.equals(command1.command)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + command.hashCode();
        return result;
    }
}
