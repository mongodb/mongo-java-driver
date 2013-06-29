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

package org.mongodb.operation;

import org.mongodb.QueryOptions;
import org.mongodb.ReadPreference;

import java.util.EnumSet;

public abstract class Query {
    private ReadPreference readPreference;
    //CHECKSTYLE:OFF
    //CHECKSTYLE:ON
    private int skip;
    private int limit;
    private QueryOptions options = new QueryOptions();
    public Query() {
    }

    public Query(final Query from) {
        readPreference = from.readPreference;
        skip = from.skip;
        limit = from.limit;
        options = new QueryOptions(from.options);
    }

    //CHECKSTYLE:OFF
    public Query readPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
        return this;
    }

    //TODO: I hate this
    public Query readPreferenceIfAbsent(final ReadPreference readPreference) {
        if (this.readPreference == null) {
            readPreference(readPreference);
        }
        return this;
    }

    public Query skip(final int skip) {
        this.skip = skip;
        return this;
    }

    public Query limit(final int limit) {
        this.limit = limit;
        return this;
    }

    public Query batchSize(final int batchSize) {
        this.getOptions().batchSize(batchSize);
        return this;
    }

    public Query addFlags(final EnumSet<QueryFlag> flags) {
        if (flags == null) {
            throw new IllegalArgumentException();
        }
        this.options.addFlags(flags);
        return this;
    }

    public Query flags(final EnumSet<QueryFlag> flags) {
        if (flags == null) {
            throw new IllegalArgumentException();
        }
        this.options.flags(flags);
        return this;
    }

    public Query options(final QueryOptions options) {
        if (options == null) {
            throw new IllegalArgumentException();
        }
        this.options = options;
        return this;
    }
    //CHECKSTYLE:ON


    public EnumSet<QueryFlag> getFlags() {
        if (readPreference != null && readPreference.isSlaveOk()) {
            EnumSet<QueryFlag> retVal = EnumSet.copyOf(options.getFlags());
            retVal.add(QueryFlag.SlaveOk);
            return retVal;
        } else {
            return options.getFlags();
        }
    }

    public ReadPreference getReadPreference() {
        return readPreference;
    }

    public int getBatchSize() {
        return options.getBatchSize();
    }

    public int getSkip() {
        return skip;
    }

    public int getLimit() {
        return limit;
    }

    public QueryOptions getOptions() {
        return options;
    }

    /**
     * Gets the limit of the number of documents in the first OP_REPLY response to the query. A value of zero tells the server to use the
     * default size. A negative value tells the server to return no more than that number and immediately close the cursor.  Otherwise, the
     * server will return no more than that number and return a cursorId to allow the rest of the documents to be fetched,
     * if it turns out there are more documents.
     * <p>
     * The value returned by this method is based on the limit and the batch size, both of which can be positive, negative, or zero.
     *
     * @return the value for numberToReturn in the OP_QUERY wire protocol message.
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public int getNumberToReturn() {
        int numberToReturn;
        if (getLimit() < 0) {
            numberToReturn = getLimit();
        }
        else if (getLimit() == 0) {
            numberToReturn = getBatchSize();
        }
        else if (getBatchSize() == 0) {
            numberToReturn = getLimit();
        }
        else if (getLimit() < Math.abs(getBatchSize())) {
            numberToReturn = getLimit();
        }
        else {
            numberToReturn = getBatchSize();
        }

        return numberToReturn;
    }

    // CHECKSTYLE:OFF
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Query that = (Query) o;

        if (limit != that.limit) return false;
        if (skip != that.skip) return false;
        if (!options.equals(that.options)) return false;
        if (readPreference != null ? !readPreference.equals(that.readPreference) : that.readPreference != null) return false;

        return true;
    }
    // CHECKSTYLE:ON

    @Override
    public int hashCode() {
        int result = readPreference != null ? readPreference.hashCode() : 0;
        result = 31 * result + skip;
        result = 31 * result + limit;
        result = 31 * result + options.hashCode();
        return result;
    }
}
