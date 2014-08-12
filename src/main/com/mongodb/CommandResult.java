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

// CommandResult.java



package com.mongodb;


import java.util.List;

/**
 * A simple wrapper for the result of getLastError() calls and other commands
 */
public class CommandResult extends BasicDBObject {

    CommandResult(ServerAddress serverAddress) {
        if (serverAddress == null) {
            throw new IllegalArgumentException("server address is null");
        }
        _host = serverAddress;
        //so it is shown in toString/debug
        put("serverUsed", serverAddress.toString());
    }

    /**
     * gets the "ok" field which is the result of the command
     * @return True if ok
     */
    public boolean ok(){
        Object okValue = get("ok");
        if (okValue instanceof Boolean) {
            return (Boolean) okValue;
        } else if (okValue instanceof Number) {
            return ((Number) okValue).intValue() == 1;
        } else {
            return false;
        }
    }

    /**
     * gets the "errmsg" field which holds the error message
     * @return The error message or null
     */
    public String getErrorMessage(){
        Object errorMessage = get( "errmsg" );
        if ( errorMessage == null )
            return null;
        return errorMessage.toString();
    }

    /**
     * utility method to create an exception with the command name
     * @return The mongo exception or null
     */
    public MongoException getException() {
        if ( !ok() ) {   // check for command failure
            if (getCode() == 50) {
                return new MongoExecutionTimeoutException(getCode(), getErrorMessage());
            }
            else {
                return new CommandFailureException( this );
            }
        } else if (hasErr()) {
            return getWriteException();
        } else {
            return null;
        }
    }

    private MongoException getWriteException() {
        int code = getCode();
        if (code == 11000 || code == 11001 || code == 12582) {
            return new MongoException.DuplicateKey(this);
        } else {
            return new WriteConcernException(this);
        }
    }

    /**
     * returns the "code" field, as an int
     * @return -1 if there is no code
     */
    @SuppressWarnings("unchecked")
    int getCode() {
        int code = getInt("code", -1);

        // mongos may return a list of documents representing getlasterror responses from each shard.  Return the one with a matching
        // "err" field, so that it can be used to get the error code
        if (code == -1 && get("errObjects") != null) {
            for (BasicDBObject curErrorDocument : (List<BasicDBObject>) get("errObjects")) {
                if (get("err").equals(curErrorDocument.get("err"))) {
                    code = curErrorDocument.getInt("code", -1);
                    break;
                }
            }
        }

        return code;
    }

    /**
     * check the "err" field
     * @return if it has it, and isn't null
     */
    boolean hasErr(){
        String err = getString("err");
        return err != null && err.length() > 0;
    }

    /**
     * throws an exception containing the cmd name, in case the command failed, or the "err/code" information
     * @throws MongoException
     */
    public void throwOnError() {
        if ( !ok() || hasErr() ){
            throw getException();
        }
    }

    /**
     * @deprecated there is no replacement for this method
     */
    @Deprecated
    public ServerAddress getServerUsed() {
        return _host;
    }

    private final ServerAddress _host;
    private static final long serialVersionUID = 1L;

}
