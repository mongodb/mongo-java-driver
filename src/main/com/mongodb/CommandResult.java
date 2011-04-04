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

    CommandResult(){
    }

    /**
     * gets the "ok" field which is the result of the command
     * @return
     */
    public boolean ok(){
        Object o = get( "ok" );
        if ( o == null )
            throw new IllegalArgumentException( "'ok' should never be null..." );

        if ( o instanceof Boolean )
            return ((Boolean)o).booleanValue();
        
        if ( o instanceof Number )
            return ((Number)o).intValue() == 1;
        
        throw new IllegalArgumentException( "can't figure out what to do with: " + o.getClass().getName() );
    }

    /**
     * gets the "errmsg" field which holds the error message
     * @return
     */
    public String getErrorMessage(){
        Object foo = get( "errmsg" );
        if ( foo == null )
            return null;
        return foo.toString();
    }
    
    /**
     * utility method to create an exception with the command name
     * @return
     */
    public MongoException getException(){
        if ( !ok() ) {
            String cmdName = _cmd.keySet().iterator().next();

            StringBuilder buf = new StringBuilder( "command failed [" );
            buf.append( "command failed [" ).append( cmdName ).append( "] " );
            buf.append( toString() );

            return new CommandFailure( this , buf.toString() );
        } else {
            // GLE check
            if ( hasErr() ) {
                Object foo = get( "err" );

                int code = getCode();

                String s = foo.toString();
                if ( code == 11000 || code == 11001 || s.startsWith( "E11000" ) || s.startsWith( "E11001" ) )
                    return new MongoException.DuplicateKey( code , s );

                return new MongoException( code , s );
            }
        }
        
        //all good, should never get here.
        return  null;
    }
 
    /**
     * returns the "code" field, as an int
     * @return -1 if there is no code
     */
    private int getCode(){
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
    public void throwOnError() throws MongoException {
        if ( !ok() || hasErr() ){
            throw getException();
        }
    }
    

    DBObject _cmd;
    private static final long serialVersionUID = 1L;

    static class CommandFailure extends MongoException {
        private static final long serialVersionUID = 1L;

        CommandFailure( CommandResult res , String msg ){
            super( ServerError.getCode( res ) , msg );
        }
    }
}
