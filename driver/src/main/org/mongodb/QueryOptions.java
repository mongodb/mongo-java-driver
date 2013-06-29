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

package org.mongodb;

import org.mongodb.operation.QueryFlag;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

public class QueryOptions {
    private final EnumSet<QueryFlag> queryFlags = EnumSet.noneOf(QueryFlag.class);
    private int batchSize;
    private long maxScan;
    private long maxTimeMS;
    private Document min;
    private Document max;
    private boolean isolated;

    public QueryOptions() {
    }

    public QueryOptions(final QueryOptions from) {
        queryFlags.clear();
        queryFlags.addAll(from.queryFlags);
        batchSize = from.batchSize;
        maxScan = from.maxScan;
        maxTimeMS = from.maxTimeMS;
        min = from.min;
        max = from.max;
        isolated = from.isolated;
    }

    public EnumSet<QueryFlag> getFlags() {
        return queryFlags;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getMaxScan() {
        return maxScan;
    }

    public long getMaxTimeMS() {
        return maxTimeMS;
    }

    public Document getMin() {
        return min;
    }

    public Document getMax() {
        return max;
    }

    public boolean isIsolated() {
        return isolated;
    }

    public QueryOptions addFlags(final EnumSet<QueryFlag> flags) {
        queryFlags.addAll(flags);
        return this;
    }

    public QueryOptions flags(final EnumSet<QueryFlag> flags) {
        queryFlags.clear();
        queryFlags.addAll(flags);
        return this;
    }

    /**
     * To isolate an update from other concurrent updates, by using the "$isolated" special field.
     */
    public QueryOptions isolate() {
        isolated = true;
        return this;
    }

    // CHECKSTYLE:OFF

    public QueryOptions batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Uses "$snapshot".
     */
    public QueryOptions snapshot() {
        return this;
    }

    public QueryOptions maxScan(final long maxScan) {
        this.maxScan = maxScan;
        return this;
    }

    public QueryOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    public QueryOptions hint(final String indexName) {
        return this;
    }

    public QueryOptions hint(final Document index) {
        return this;
    }

    public QueryOptions hint(final ConvertibleToDocument index) {
        return this;
    }

    public QueryOptions min(final Document min) {
        this.min = min;
        return this;
    }

    public QueryOptions max(final Document max) {
        this.max = max;
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final QueryOptions that = (QueryOptions) o;

        if (batchSize != that.batchSize) return false;
        if (isolated != that.isolated) return false;
        if (maxScan != that.maxScan) return false;
        if (maxTimeMS != that.maxTimeMS) return false;
        if (max != null ? !max.equals(that.max) : that.max != null) return false;
        if (min != null ? !min.equals(that.min) : that.min != null) return false;
        if (!queryFlags.equals(that.queryFlags)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = queryFlags.hashCode();
        result = 31 * result + batchSize;
        result = 31 * result + (int) (maxScan ^ (maxScan >>> 32));
        result = 31 * result + (int) (maxTimeMS ^ (maxTimeMS >>> 32));
        result = 31 * result + (min != null ? min.hashCode() : 0);
        result = 31 * result + (max != null ? max.hashCode() : 0);
        result = 31 * result + (isolated ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "QueryOptions{"
                + "queryFlags=" + queryFlags
                + ", batchSize=" + batchSize
                + ", maxScan=" + maxScan
                + ", maxTimeMS=" + maxTimeMS
                + ", min=" + min
                + ", max=" + max
                + ", isolated=" + isolated
                + '}';
    }
}
