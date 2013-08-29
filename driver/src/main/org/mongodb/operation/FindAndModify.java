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

public abstract class FindAndModify extends Query {
    private Document filter;
    private Document selector;
    private Document sortCriteria;
    private boolean returnNew;
    private boolean upsert;

    public FindAndModify() {
    }

    //CHECKSTYLE:OFF
    public FindAndModify where(final Document filter) {
        this.filter = filter;
        return this;
    }

    public FindAndModify select(final Document selector) {
        this.selector = selector;
        return this;
    }

    public FindAndModify sortBy(final Document sortCriteria) {
        this.sortCriteria = sortCriteria;
        return this;
    }

    public FindAndModify returnNew(final boolean returnNew) {
        this.returnNew = returnNew;
        return this;
    }

    // TODO: doesn't make sense for find and remove
    public FindAndModify upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }
    //CHECKSTYLE:ON

    public Document getFilter() {
        return filter;
    }

    public Document getSelector() {
        return selector;
    }

    public Document getSortCriteria() {
        return sortCriteria;
    }

    // TODO: Doesn't make sense for find and remove
    public boolean isReturnNew() {
        return returnNew;
    }

    public boolean isUpsert() {
        return upsert;
    }
}
