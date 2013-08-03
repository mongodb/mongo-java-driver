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

package org.mongodb.command;

import org.mongodb.CommandResult;
import org.mongodb.ReadPreference;
import org.mongodb.WriteConcern;

import java.util.Arrays;
import java.util.List;

/**
 * The getlasterror command.
 */
public final class GetLastError extends Command {

    private static final List<Integer> DUPLICATE_KEY_ERROR_CODES = Arrays.asList(11000, 11001, 12582);

    public GetLastError(final WriteConcern writeConcern) {
        super(writeConcern.getCommand());
        readPreference(ReadPreference.primary());
    }

    public static CommandResult parseGetLastErrorResponse(final CommandResult commandResult) {
        MongoCommandFailureException exception = getCommandException(commandResult);
        if (exception != null) {
            throw exception;
        }
        return commandResult;
    }

    public static MongoCommandFailureException getCommandException(final CommandResult commandResult) {
        final Integer code = (Integer) commandResult.getResponse().get("code");
        if (DUPLICATE_KEY_ERROR_CODES.contains(code)) {
            return new MongoDuplicateKeyException(commandResult);
        }
        return null;
    }
}
