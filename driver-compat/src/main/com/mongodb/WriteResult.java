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

package com.mongodb;


import org.mongodb.command.GetLastError;

public class WriteResult {

    private final com.mongodb.WriteConcern writeConcern;
    private final DB db;
    private CommandResult getLastErrorResult;

    WriteResult(final org.mongodb.result.WriteResult result, final WriteConcern writeConcern, final DB db) {
        this.writeConcern = writeConcern;
        this.db = db;
        if (result.getGetLastErrorResult() != null) {
            getLastErrorResult = toGetLastErrorResult(result.getGetLastErrorResult());
        }
    }


    public com.mongodb.CommandResult getLastError() {
        if (getLastErrorResult == null) {
            org.mongodb.result.CommandResult commandResult
                    = db.executeCommand(new GetLastError(writeConcern.toNew()));
            getLastErrorResult = toGetLastErrorResult(commandResult);
        }

        return getLastErrorResult;
    }

    private CommandResult toGetLastErrorResult(final org.mongodb.result.CommandResult commandResult) {
        // TODO: need command and server address fer realz
        return DBObjects
                .toCommandResult(DBObjects.toDBObject(commandResult.getCommand()),
                        new ServerAddress(commandResult.getAddress()),
                        commandResult.getResponse());
    }

    public WriteConcern getLastConcern() {
        return writeConcern;
    }

    public CommandResult getCachedLastError() {
        return getLastErrorResult;
    }

    /**
     * Gets the "n" field, which contains the number of documents
     * affected in the write operation.
     *
     * @return number of documents affected in the write operation
     * @throws MongoException
     */
    public int getN() {
        return getLastErrorResult.getInt("n");
    }
}
