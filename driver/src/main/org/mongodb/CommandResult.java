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

import org.mongodb.connection.ServerAddress;

public class CommandResult {
    private final Document command;
    private final ServerAddress address;
    private final Document response;
    private final long elapsedNanoseconds;

    public CommandResult(final Document command, final ServerAddress address, final Document response, final long elapsedNanoseconds) {
        this.command = command;
        this.address = address;
        this.response = response;
        this.elapsedNanoseconds = elapsedNanoseconds;
    }

    public CommandResult(final CommandResult baseResult) {
        this.command = baseResult.command;
        this.address = baseResult.address;
        this.response = baseResult.response;
        this.elapsedNanoseconds = baseResult.elapsedNanoseconds;
    }

    public Document getCommand() {
        return command;
    }

    public ServerAddress getAddress() {
        return address;
    }

    public Document getResponse() {
        return response;
    }

    public boolean isOk() {
        return getResponse().get("ok").equals(1.0);
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
