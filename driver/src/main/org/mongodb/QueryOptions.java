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


package org.mongodb;


import org.mongodb.operation.QueryFlag;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


public class QueryOptions {
    private final EnumSet<QueryFlag> queryFlags = EnumSet.noneOf(QueryFlag.class);
    private int batchSize;
    private int maxScan;
    private long maxTimeMS;
    private Document min;
    private Document max;
    private boolean isolated;
    private String comment;
    private boolean explain;
    private Document hint;
    private boolean returnKey;
    private boolean showDiskLoc;
    private boolean snapshot;

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

    public String getComment() {
        return comment;
    }

    public Document getHint() {
        return hint;
    }

    public int getMaxScan() {
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

    public long getMaxTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxTimeMS, MILLISECONDS);
    }

    public boolean isExplain() {
        return explain;
    }

    public boolean isIsolated() {
        return isolated;
    }

    public boolean isReturnKey() {
        return returnKey;
    }

    public boolean isShowDiskLoc() {
        return showDiskLoc;
    }

    public boolean isSnapshot() {
        return snapshot;
    }

    public QueryOptions addFlags(final EnumSet<QueryFlag> flags) {
        queryFlags.addAll(flags);
        return this;
    }

    // CHECKSTYLE:OFF

    public QueryOptions batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }
    
    public QueryOptions comment(final String comment) {
        this.comment = comment;
        return this;
    }
    
    public QueryOptions explain() {
        explain = true;
        return this;
    }

    public QueryOptions flags(final EnumSet<QueryFlag> flags) {
        queryFlags.clear();
        queryFlags.addAll(flags);
        return this;
    }

    public QueryOptions hint(final String indexName) {
        hint = new Document(indexName, 1);
        return this;
    }

    public QueryOptions hint(final Document index) {
        hint = index;
        return this;
    }

    public QueryOptions hint(final ConvertibleToDocument index) {
        hint = index.toDocument();
        return this;
    }

    /**
     * To isolate an update from other concurrent updates, by using the "$isolated" special field.
     */
    public QueryOptions isolate() {
        isolated = true;
        return this;
    }

    public QueryOptions max(final Document max) {
        this.max = max;
        return this;
    }

    public QueryOptions maxScan(final int maxScan) {
        this.maxScan = maxScan;
        return this;
    }

    public QueryOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    public QueryOptions min(final Document min) {
        this.min = min;
        return this;
    }

    public QueryOptions returnKey() {
        returnKey = true;
        return this;
    }
    
    public QueryOptions showDiskLoc() {
        showDiskLoc = true;
        return this;
    }
    
    public QueryOptions snapshot() {
        snapshot = true;
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QueryOptions)) {
            return false;
        }

        final QueryOptions that = (QueryOptions) o;

        if (batchSize != that.batchSize) {
            return false;
        }
        if (explain != that.explain) {
            return false;
        }
        if (isolated != that.isolated) {
            return false;
        }
        if (maxScan != that.maxScan) {
            return false;
        }
        if (maxTimeMS != that.maxTimeMS) {
            return false;
        }
        if (returnKey != that.returnKey) {
            return false;
        }
        if (showDiskLoc != that.showDiskLoc) {
            return false;
        }
        if (snapshot != that.snapshot) {
            return false;
        }
        if (comment != null ? !comment.equals(that.comment) : that.comment != null) {
            return false;
        }
        if (hint != null ? !hint.equals(that.hint) : that.hint != null) {
            return false;
        }
        if (max != null ? !max.equals(that.max) : that.max != null) {
            return false;
        }
        if (min != null ? !min.equals(that.min) : that.min != null) {
            return false;
        }
        if (queryFlags != null ? !queryFlags.equals(that.queryFlags) : that.queryFlags != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = queryFlags != null ? queryFlags.hashCode() : 0;
        result = 31 * result + batchSize;
        result = 31 * result + maxScan;
        result = 31 * result + (int) (maxTimeMS ^ (maxTimeMS >>> 32));
        result = 31 * result + (min != null ? min.hashCode() : 0);
        result = 31 * result + (max != null ? max.hashCode() : 0);
        result = 31 * result + (isolated ? 1 : 0);
        result = 31 * result + (comment != null ? comment.hashCode() : 0);
        result = 31 * result + (explain ? 1 : 0);
        result = 31 * result + (hint != null ? hint.hashCode() : 0);
        result = 31 * result + (returnKey ? 1 : 0);
        result = 31 * result + (showDiskLoc ? 1 : 0);
        result = 31 * result + (snapshot ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("QueryOptions{");
        sb.append("queryFlags=").append(queryFlags);
        sb.append(", batchSize=").append(batchSize);
        sb.append(", maxScan=").append(maxScan);
        sb.append(", maxTimeMS=").append(maxTimeMS);
        sb.append(", min=").append(min);
        sb.append(", max=").append(max);
        sb.append(", isolated=").append(isolated);
        sb.append(", comment='").append(comment).append('\'');
        sb.append(", explain=").append(explain);
        sb.append(", hint=").append(hint);
        sb.append(", returnKey=").append(returnKey);
        sb.append(", showDiskLoc=").append(showDiskLoc);
        sb.append(", snapshot=").append(snapshot);
        sb.append('}');
        return sb.toString();
    }
}
