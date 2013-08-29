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

public class FindAndUpdate<T> extends FindAndModify {
    private Document updateOperations;

    public FindAndUpdate() {
    }

    public FindAndUpdate<T> updateWith(final Document anUpdateOperations) {
        this.updateOperations = anUpdateOperations;
        return this;
    }

    public Document getUpdateOperations() {
        return updateOperations;
    }

    @Override
    public FindAndUpdate<T> where(final Document filter) {
        super.where(filter);
        return this;
    }

    @Override
    public FindAndUpdate<T> select(final Document selector) {
        super.select(selector);
        return this;
    }

    @Override
    public FindAndUpdate<T> sortBy(final Document sortCriteria) {
        super.sortBy(sortCriteria);
        return this;
    }

    @Override
    public FindAndUpdate<T> returnNew(final boolean returnNew) {
        super.returnNew(returnNew);
        return this;
    }

    @Override
    public FindAndUpdate<T> upsert(final boolean upsert) {
        super.upsert(upsert);
        return this;
    }

}
