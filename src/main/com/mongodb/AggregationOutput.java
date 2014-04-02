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

package com.mongodb;

@SuppressWarnings("deprecation")
public class AggregationOutput {

    /**
     * returns an iterator to the results of the aggregation
     * @return the results of the aggregation
     */
    public Iterable<DBObject> results() {
        return _resultSet;
    }
    
    /**
     * returns the command result of the aggregation
     * @return the command result
     *
     * @deprecated there is no replacement for this method
     */
    @Deprecated
    public CommandResult getCommandResult(){
        return _commandResult;
    }

    /**
     * returns the original aggregation command
     * @return the command
     *
     * @deprecated there is no replacement for this method
     */
    @Deprecated
    public DBObject getCommand() {
        return _cmd;
    }

    /**
     * returns the address of the server used to execute the aggregation
     * @return the server which executed the aggregation
     *
     * @deprecated there is no replacement for this method
     */
    @Deprecated
    public ServerAddress getServerUsed() {
        return _commandResult.getServerUsed();
    }

    /**
     * Constructs a new instance
     *
     * @param command the aggregation command
     * @param commandResult the aggregation command result
     *
     * @deprecated there is no replacement for this constructor
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public AggregationOutput(DBObject command, CommandResult commandResult) {
        _commandResult = commandResult;
        _cmd = command;
        
        if(commandResult.containsField("result"))
            _resultSet = (Iterable<DBObject>) commandResult.get( "result" );
        else 
            throw new IllegalArgumentException("result undefined");
    }

    /**
     * @deprecated Please use {@link #getCommandResult()} instead.
     */
    @Deprecated
    protected final CommandResult _commandResult;

    /**
     * @deprecated Please use {@link #getCommand()} instead.
     */
    @Deprecated
    protected final DBObject _cmd;

    /**
     * @deprecated Please use {@link #results()} instead.
     */
    @Deprecated
    protected final Iterable<DBObject> _resultSet;
}