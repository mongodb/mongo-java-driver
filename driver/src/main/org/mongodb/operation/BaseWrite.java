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

import org.mongodb.WriteConcern;

public abstract class BaseWrite {
    private WriteConcern writeConcern;

    // TODO: discuss this builder pattern.  It doesn't work so well with subclasses
    //CHECKSTYLE:OFF
    public BaseWrite writeConcern(final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
        return this;
    }
    //CHECKSTYLE:ON

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }
}
