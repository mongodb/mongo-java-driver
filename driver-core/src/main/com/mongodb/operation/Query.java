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


package com.mongodb.operation;


import com.mongodb.ReadPreference;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

/**
 * This class defines the parameters required for performing Query operations against MongoDB.
 *
 * @mongodb.driver.manual manual/core/read-operations-introduction/ Read Operations
 * @since 3.0
 */
public abstract class Query {
    private int skip;
    private int limit;
    private QueryOptions options = new QueryOptions();
    private EnumSet<QueryFlag> flags = EnumSet.noneOf(QueryFlag.class);

    /**
     * Creates a new Query with the default settings.
     */
    public Query() {
    }

    /**
     * Creates a new Query, copying the settings from an existing Query
     *
     * @param from a Query containing the values to copy for this query.
     */
    public Query(final Query from) {
        skip = from.skip;
        limit = from.limit;
        options = new QueryOptions(from.options);
        flags = EnumSet.copyOf(from.flags);
    }

    /**
     * The number of elements to discard at the beginning of the query.
     *
     * @param skip the number of elements to skip
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual manual/reference/method/cursor.skip/
     */
    public Query skip(final int skip) {
        this.skip = skip;
        return this;
    }

    /**
     * Limits the number of elements returned. Note: parameter {@code limit} should be positive, although a negative value is supported for
     * legacy reason.
     *
     * @param limit the number of elements to return
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual manual/reference/method/cursor.limit/
     */
    public Query limit(final int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Limits the number of elements returned in one batch.
     *
     * @param batchSize the number of elements to return in a batch
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual manual/reference/method/cursor.batchSize/
     */
    public Query batchSize(final int batchSize) {
        this.getOptions().batchSize(batchSize);
        return this;
    }

    /**
     * Adds these query flags to this query.  Does not over-write any existing flags.
     *
     * @param flags a set of all the OP_QUERY query options for this query.
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public Query addFlags(final EnumSet<QueryFlag> flags) {
        if (flags == null) {
            throw new IllegalArgumentException();
        }
        this.flags.addAll(flags);
        return this;
    }

    /**
     * Sets these query flags on this query.  Over-writes any existing flags.
     *
     * @param flags a set of all the OP_QUERY query options for this query.
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public Query flags(final EnumSet<QueryFlag> flags) {
        if (flags == null) {
            throw new IllegalArgumentException();
        }
        this.flags.clear();
        this.flags.addAll(flags);
        return this;
    }

    /**
     * Sets the given QueryOptions on this Query.
     *
     * @param options additional options for performing this query.
     * @return {@code this} so calls can be chained
     */
    public Query options(final QueryOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("Query options can not be null.");
        }
        this.options = options;
        return this;
    }

    /**
     * This specifies a cumulative time limit for processing operations on the cursor. MongoDB interrupts the operation at the earliest
     * following interrupt point.
     *
     * @param maxTime  the time limit for processing the query operation
     * @param timeUnit the TimeUnit for this limit
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual manual/reference/operator/meta/maxTimeMS/ $maxTimeMS
     * @mongodb.server.release 2.6
     */
    public Query maxTime(final long maxTime, final TimeUnit timeUnit) {
        options.maxTime(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets the flags that have been set on this query.
     *
     * @param readPreference Where to execute the query.
     * @return a set of all the OP_QUERY query options for this query.
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public EnumSet<QueryFlag> getFlags(final ReadPreference readPreference) {
        if (readPreference.isSlaveOk()) {
            EnumSet<QueryFlag> retVal = EnumSet.copyOf(flags);
            retVal.add(QueryFlag.SlaveOk);
            return retVal;
        } else {
            return flags;
        }
    }

    /**
     * Get the batch size for this operation.
     *
     * @return the number of documents to return per batch.
     */
    public int getBatchSize() {
        return options.getBatchSize();
    }

    /**
     * Gets the skip value.
     *
     * @return the number of elements to discard at the beginning of the query.
     */
    public int getSkip() {
        return skip;
    }

    /**
     * Gets the limit value.
     *
     * @return the maximum number of documents to return when executing this query.
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Gets a QueryOptions that contains options for running this query.
     *
     * @return additional options for performing this query.
     */
    public QueryOptions getOptions() {
        return options;
    }

    /**
     * <p>Gets the limit of the number of documents in the first OP_REPLY response to the query. A value of zero tells the server to use the
     * default size. A negative value tells the server to return no more than that number and immediately close the cursor.  Otherwise, the
     * server will return no more than that number and return a cursorId to allow the rest of the documents to be fetched, if it turns out
     * there are more documents.</p>
     *
     * <p>The value returned by this method is based on the limit and the batch size, both of which can be positive, negative, or zero.</p>
     *
     * @return the value for numberToReturn in the OP_QUERY wire protocol message.
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public int getNumberToReturn() {
        int numberToReturn;
        if (getLimit() < 0) {
            numberToReturn = getLimit();
        } else if (getLimit() == 0) {
            numberToReturn = getBatchSize();
        } else if (getBatchSize() == 0) {
            numberToReturn = getLimit();
        } else if (getLimit() < Math.abs(getBatchSize())) {
            numberToReturn = getLimit();
        } else {
            numberToReturn = getBatchSize();
        }

        return numberToReturn;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Query query = (Query) o;

        if (limit != query.limit) {
            return false;
        }
        if (skip != query.skip) {
            return false;
        }
        if (!flags.equals(query.flags)) {
            return false;
        }
        if (!options.equals(query.options)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = skip;
        result = 31 * result + limit;
        result = 31 * result + options.hashCode();
        result = 31 * result + flags.hashCode();
        return result;
    }
}
