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

import org.mongodb.Document;
import org.mongodb.ReadPreference;

import java.util.EnumSet;

public class Find extends Query {
    private Document filter;
    private Document fields;
    private Document sortCriteria;
    private boolean snapshotMode;
    private boolean explain;

    public Find() {
        this(new Document());
    }

    public Find(final Document filter) {
        this.filter = filter;
        readPreference(ReadPreference.primary());
    }

    public Find(final Find from) {
        super(from);
        filter = from.filter;
        fields = from.fields;
        sortCriteria = from.sortCriteria;
        snapshotMode = from.snapshotMode;
    }

    public Document getFilter() {
        return filter;
    }

    public Document getOrder() {
        return sortCriteria;
    }

    public boolean isSnapshotMode() {
        return snapshotMode;
    }

    public boolean isExplain() {
        return explain;
    }

    //CHECKSTYLE:OFF
    //I think we're going to have to turn off "hides a field" unless we can work out how to ignore it for builders
    public Find where(final Document filter) {
        this.filter = filter;
        return this;
    }

    public Find filter(final Document filter) {
        this.filter = filter;
        return this;
    }

    public Find select(final Document fields) {
        this.fields = fields;
        return this;
    }

    public Find order(final Document sortCriteria) {
        this.sortCriteria = sortCriteria;
        return this;
    }
    //CHECKSTYLE:ON

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
    public Find addFlags(final EnumSet<QueryFlag> flags) {
        super.addFlags(flags);
        return this;
    }

    public Find hintIndex(final String indexName) {
        throw new UnsupportedOperationException();      // TODO
    }

    public Find snapshot() {
        this.snapshotMode = true;
        return this;
    }

    public Document getFields() {
        return fields;
    }

    public Find readPreference(final ReadPreference readPreference) {
        super.readPreference(readPreference);
        return this;
    }

    public Find readPreferenceIfAbsent(final ReadPreference readPreference) {
        super.readPreferenceIfAbsent(readPreference);
        return this;
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

    // CHECKSTYLE:OFF
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        final Find find = (Find) o;

        if (explain != find.explain) return false;
        if (snapshotMode != find.snapshotMode) return false;
        if (fields != null ? !fields.equals(find.fields) : find.fields != null) return false;
        if (filter != null ? !filter.equals(find.filter) : find.filter != null) return false;
        if (sortCriteria != null ? !sortCriteria.equals(find.sortCriteria) : find.sortCriteria != null) return false;

        return true;
    }
    // CHECKSTYLE:ON

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        result = 31 * result + (sortCriteria != null ? sortCriteria.hashCode() : 0);
        result = 31 * result + (snapshotMode ? 1 : 0);
        result = 31 * result + (explain ? 1 : 0);
        return result;
    }
}
