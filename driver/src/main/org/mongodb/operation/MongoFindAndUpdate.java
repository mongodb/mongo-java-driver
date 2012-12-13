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

public class MongoFindAndUpdate extends MongoFindAndModify {
    private MongoUpdateOperations updateOperations;

    public MongoFindAndUpdate updateWith(final MongoUpdateOperations updateOperations) {
        this.updateOperations = updateOperations;
        return this;
    }

    public MongoUpdateOperations getUpdateOperations() {
        return updateOperations;
    }

    @Override
    public MongoFindAndUpdate where(final MongoQueryFilter filter) {
        super.where(filter);
        return this;
    }

    @Override
    public MongoFindAndUpdate select(final MongoFieldSelector selector) {
        super.select(selector);
        return this;
    }

    @Override
    public MongoFindAndUpdate sortBy(final MongoSortCriteria sortCriteria) {
        super.sortBy(sortCriteria);
        return this;
    }

    @Override
    public MongoFindAndUpdate returnNew(final boolean returnNew) {
        super.returnNew(returnNew);
        return this;
    }

    @Override
    public MongoFindAndUpdate upsert(final boolean upsert) {
        super.upsert(upsert);
        return this;
    }

}
