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

import org.mongodb.WriteConcern;

public class MongoUpdate extends MongoWrite {
    private final MongoQueryFilter filter;
    private final MongoUpdateOperations updateOperations;
    private boolean isMulti = false;
    private boolean isUpsert = false;

    public MongoUpdate(MongoQueryFilter filter, MongoUpdateOperations updateOperations) {

        this.filter = filter;
        this.updateOperations = updateOperations;
    }

    public MongoQueryFilter getFilter() {
        return filter;
    }

    public MongoUpdateOperations getUpdateOperations() {
        return updateOperations;
    }

    public boolean isMulti() {
        return isMulti;
    }

    public boolean isUpsert() {
        return isUpsert;
    }

    MongoUpdate isMulti(final boolean isMulti) {
        this.isMulti = isMulti;
        return this;
    }

    MongoUpdate isUpsert(final boolean isUpsert) {
        this.isUpsert = isUpsert;
        return this;
    }

    @Override
    public MongoUpdate writeConcern(final WriteConcern writeConcern) {
        super.writeConcern(writeConcern);
        return this;
    }

    @Override
    public MongoUpdate writeConcernIfAbsent(final WriteConcern writeConcern) {
        super.writeConcernIfAbsent(writeConcern);
        return this;
    }
}

