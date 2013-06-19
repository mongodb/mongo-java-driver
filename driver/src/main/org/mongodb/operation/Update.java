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
import org.mongodb.WriteConcern;

public class Update extends BaseUpdate {
    private final Document updateOperations;
    private boolean isMulti = false;

    public Update(final Document filter, final Document updateOperations, final WriteConcern writeConcern) {
        super(filter, writeConcern);

        this.updateOperations = updateOperations;
    }

    public Document getUpdateOperations() {
        return updateOperations;
    }

    public boolean isMulti() {
        return isMulti;
    }

    //CHECKSTYLE:OFF
    public Update multi(final boolean isMulti) {
        this.isMulti = isMulti;
        return this;
    }
    //CHECKSTYLE:ON

    @Override
    public Update upsert(final boolean isUpsert) {
        super.upsert(isUpsert);
        return this;
    }
}

