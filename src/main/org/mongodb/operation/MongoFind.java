/**
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

public class MongoFind extends MongoQuery {
    private MongoQueryFilter filter;
    private MongoFieldSelector fields;

    public MongoFind() {
    }

    public MongoFind(final MongoQueryFilter filter) {
        this.filter = filter;
    }

    public MongoQueryFilter getFilter() {
        return filter;
    }

    public MongoFind where(final MongoQueryFilter filter) {
        this.filter = filter;
        return this;
    }

    // TODO: Add MongoQuerySelector interface
    public MongoFind select(final MongoFieldSelector fields) {
        this.fields = fields;
        return this;
    }


    public MongoFind order(final String condition) {
        throw new UnsupportedOperationException();
    }

    public MongoFind limit(final int value) {
        throw new UnsupportedOperationException();
    }

    public MongoFind batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public MongoFind offset(final int value) {
        throw new UnsupportedOperationException();
    }

    public MongoFind hintIndex(final String idxName) {
        throw new UnsupportedOperationException();
    }

    public MongoFind enableSnapshotMode() {
        throw new UnsupportedOperationException();
    }

    public MongoFind disableSnapshotMode() {
        throw new UnsupportedOperationException();
    }

    public MongoFind disableTimeout() {
        throw new UnsupportedOperationException();
    }

    public MongoFind enableTimeout() {
        throw new UnsupportedOperationException();
    }

    public MongoFieldSelector getFields() {
        return fields;
    }

    public MongoFind readPreference(final ReadPreference readPreference) {
        super.readPreference(readPreference);
        return this;
    }

    public MongoFind readPreferenceIfAbsent(final ReadPreference readPreference) {
        super.readPreferenceIfAbsent(readPreference);
        return this;
    }

}
