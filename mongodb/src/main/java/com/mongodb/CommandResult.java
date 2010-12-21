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

/** A simple wrapper for the result of getLastError() calls, and network (socket) errors. */
public class CommandResult extends BasicDBObject {

    CommandResult(){
    }

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

    public String getErrorMessage(){
        Object foo = get( "errmsg" );
        if ( foo == null )
            return null;
        return foo.toString();
    }
    
    public MongoException getException(){
        String cmdName = _cmd.keySet().iterator().next();

        StringBuilder buf = new StringBuilder( "command failed [" );
        buf.append( "command failed [" ).append( cmdName ).append( "] " );
        buf.append( toString() );
        
        return new CommandFailure( this , buf.toString() );
    }

    public void throwOnError()
        throws MongoException {
        if ( ! ok() ){
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
