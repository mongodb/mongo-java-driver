/*
 * Copyright (c) 2008 MongoDB, Inc.
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

public class AggregationOutput {

    /**
     * returns an iterator to the results of the aggregation
     * @return
     */
    public Iterable<DBObject> results() {
        return _resultSet;
    }
    
    /**
     * returns the command result of the aggregation
     * @return
     */
    public CommandResult getCommandResult(){
        return _commandResult;
    }

    /**
     * returns the original aggregation command
     * @return
     */
    public DBObject getCommand() {
        return _cmd;
    }

    /**
     * returns the address of the server used to execute the aggregation
     * @return
     */
    public ServerAddress getServerUsed() {
        return _commandResult.getServerUsed();
    }

    /**
     * string representation of the aggregation command
     */
    public String toString(){
        return _commandResult.toString();
    }
   
    @SuppressWarnings("unchecked")
    public AggregationOutput(DBObject cmd, CommandResult raw) {
        _commandResult = raw;
        _cmd = cmd;
        
        if(raw.containsField("result"))
            _resultSet = (Iterable<DBObject>) raw.get( "result" );
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