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

public abstract class BaseUpdate extends BaseWrite {
    private final Document filter;
    private boolean isUpsert = false;

    public BaseUpdate(final Document filter) {
        this.filter = filter;
    }

    public Document getFilter() {
        return filter;
    }

    public boolean isUpsert() {
        return isUpsert;
    }

    //CHECKSTYLE:OFF
    public BaseUpdate upsert(final boolean isUpsert) {
        this.isUpsert = isUpsert;
        return this;
    }
    //CHECKSTYLE:ON

    public abstract boolean isMulti();
}
