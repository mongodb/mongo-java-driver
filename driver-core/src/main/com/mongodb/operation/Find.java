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

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

public class Find extends Query {
    private BsonDocument filter;
    private BsonDocument fields;
    private BsonDocument sortCriteria;
    private BsonValue hint;
    private boolean snapshotMode;
    private boolean explain;

    public Find() {
        this(new BsonDocument());
    }

    public Find(final BsonDocument filter) {
        this.filter = filter;
    }

    public Find(final Find from) {
        super(from);
        filter = from.filter;
        fields = from.fields;
        hint = from.hint;
        sortCriteria = from.sortCriteria;
        snapshotMode = from.snapshotMode;
    }

    public BsonDocument getFilter() {
        return filter;
    }

    public BsonDocument getOrder() {
        return sortCriteria;
    }

    public BsonValue getHint() {
        return hint;
    }

    public boolean isSnapshotMode() {
        return snapshotMode;
    }

    public boolean isExplain() {
        return explain;
    }

    public Find where(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    public Find filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    public Find select(final BsonDocument fields) {
        this.fields = fields;
        return this;
    }

    public Find order(final BsonDocument sortCriteria) {
        this.sortCriteria = sortCriteria;
        return this;
    }

    @Override
    public Find limit(final int limit) {
        super.limit(limit);
        return this;
    }

    @Override
    public Find batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public Find maxTime(final long maxTime, final TimeUnit timeUnit) {
        super.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public Find addFlags(final EnumSet<QueryFlag> flags) {
        super.addFlags(flags);
        return this;
    }

    public Find hintIndex(final String indexName) {
        this.hint = new BsonString(indexName);
        return this;
    }

    public Find hintIndex(final BsonDocument keys) {
        this.hint = keys;
        return this;
    }

    public Find snapshot() {
        this.snapshotMode = true;
        return this;
    }

    public BsonDocument getFields() {
        return fields;
    }

    @Override
    public Find skip(final int skip) {
        super.skip(skip);
        return this;
    }

    public Find explain() {
        explain = true;
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

        Find find = (Find) o;

        if (explain != find.explain) {
            return false;
        }
        if (snapshotMode != find.snapshotMode) {
            return false;
        }
        if (fields != null ? !fields.equals(find.fields) : find.fields != null) {
            return false;
        }
        if (filter != null ? !filter.equals(find.filter) : find.filter != null) {
            return false;
        }
        if (hint != null ? !hint.equals(find.hint) : find.hint != null) {
            return false;
        }
        if (sortCriteria != null ? !sortCriteria.equals(find.sortCriteria) : find.sortCriteria != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        result = 31 * result + (sortCriteria != null ? sortCriteria.hashCode() : 0);
        result = 31 * result + (hint != null ? hint.hashCode() : 0);
        result = 31 * result + (snapshotMode ? 1 : 0);
        result = 31 * result + (explain ? 1 : 0);
        return result;
    }
}
