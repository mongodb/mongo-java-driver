/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.connection;

import com.mongodb.ReadConcern;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A SessionContext implementation that does nothing and reports that it has no session, but does track read concern.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class ReadConcernAwareNoOpSessionContext extends NoOpSessionContext {

    private final ReadConcern readConcern;

    public ReadConcernAwareNoOpSessionContext(final ReadConcern readConcern) {
        this.readConcern = notNull("readConcern", readConcern);
    }

    @Override
    public ReadConcern getReadConcern() {
        return readConcern;
    }
}
