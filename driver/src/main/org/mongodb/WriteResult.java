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

package org.mongodb;

/**
 *
 * @since 3.0
 */
public class WriteResult {
    private final CommandResult getLastErrorResult;
    private WriteConcern writeConcern;

    public WriteResult(final CommandResult commandResult, final WriteConcern writeConcern) {
        this.getLastErrorResult = commandResult;
        this.writeConcern = writeConcern;
    }

    public CommandResult getResult() {
        return getLastErrorResult;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public String toString() {
        return "WriteResult{"
                + "getLastErrorResult=" + getLastErrorResult
                + ", writeConcern=" + writeConcern
                + '}';
    }
}
