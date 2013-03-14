// CommandResult.java
/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */


package com.mongodb;


/**
 * A simple wrapper for the result of getLastError() calls and other commands
 */
public class CommandResult extends BasicDBObject {

    CommandResult(ServerAddress srv) {
        if (srv == null) {
            throw new IllegalArgumentException("server address is null");
        }
        _host = srv;
        //so it is shown in toString/debug
        put("serverUsed", srv.toString());
    }

    /**
     * gets the "ok" field which is the result of the command
     * @return True if ok
     */
    public boolean ok(){
        Object o = get( "ok" );
        if ( o == null )
            throw new IllegalArgumentException( "'ok' should never be null..." );

        if ( o instanceof Boolean )
            return (Boolean) o;

        if ( o instanceof Number )
            return ((Number)o).intValue() == 1;

        throw new IllegalArgumentException( "can't figure out what to do with: " + o.getClass().getName() );
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
            return new CommandFailureException( this );
        } else if ( hasErr() ) { // check for errors reported by getlasterror command
            if (getCode() == 11000 || getCode() == 11001 || getCode() == 12582) {
                return new MongoException.DuplicateKey(this);
            }
            else {
                return new WriteConcernException(this);
            }
        }

        return null;
    }

    /**
     * returns the "code" field, as an int
     * @return -1 if there is no code
     */
    int getCode() {
        int code = -1;
        if ( get( "code" ) instanceof Number )
            code = ((Number)get("code")).intValue();
        return code;
    }

    /**
     * check the "err" field
     * @return if it has it, and isn't null
     */
    boolean hasErr(){
        Object o = get( "err" );
        return (o != null && ( (String) o ).length() > 0 );
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

    public ServerAddress getServerUsed() {
	return _host;
    }

    private final ServerAddress _host;
    private static final long serialVersionUID = 1L;

}
