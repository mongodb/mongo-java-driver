/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.mongodb.connection.ServerAddress;

public class CommandResult {
    private final ServerAddress address;
    private final Document response;
    private final long elapsedNanoseconds;

    public CommandResult(final ServerAddress address, final Document response, final long elapsedNanoseconds) {
        this.address = address;
        this.response = response;
        this.elapsedNanoseconds = elapsedNanoseconds;
    }

    public CommandResult(final CommandResult baseResult) {
        this.address = baseResult.address;
        this.response = baseResult.response;
        this.elapsedNanoseconds = baseResult.elapsedNanoseconds;
    }

    public ServerAddress getAddress() {
        return address;
    }

    public Document getResponse() {
        return response;
    }

    /**
     * Return true if the command completed successfully.
     *
     * @return true if the command completed successfully, false otherwise.
     */
    public boolean isOk() {
        Object okValue = response.get("ok");
        if (okValue instanceof Boolean) {
            return (Boolean) okValue;
        } else if (okValue instanceof Number) {
            return ((Number) okValue).intValue() == 1;
        } else {
            return false;
        }
    }

    public int getErrorCode() {
        Integer errorCode = (Integer) getResponse().get("code");
        return (errorCode != null) ? errorCode : -1;
    }

    public String getErrorMessage() {
        return (String) getResponse().get("errmsg");
    }

    public long getElapsedNanoseconds() {
        return elapsedNanoseconds;
    }
}
