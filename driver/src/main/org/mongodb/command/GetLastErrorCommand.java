/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

import org.mongodb.MongoDatabase;
import org.mongodb.WriteConcern;
import org.mongodb.operation.MongoCommand;
import org.mongodb.result.CommandResult;

import java.util.Arrays;
import java.util.List;

/**
 * The getlasterror command.
 */
public class GetLastErrorCommand extends AbstractCommand {
    private final WriteConcern writeConcern;

    // TODO: there are more of these...
    private static final List<Integer> DUPLICATE_KEY_ERROR_CODES = Arrays.asList(11000);

    public GetLastErrorCommand(final MongoDatabase database, final WriteConcern writeConcern) {
        super(database);
        this.writeConcern = writeConcern;
    }

    @Override
    public CommandResult execute() {
        final CommandResult res = super.execute();
        final Integer code = (Integer) res.getResponse().get("code");
        if (DUPLICATE_KEY_ERROR_CODES.contains(code)) {
            throw new MongoDuplicateKeyException(res);
        }

        return res;
    }

    @Override
    public MongoCommand asMongoCommand() {
        return writeConcern.getCommand();
    }
}
