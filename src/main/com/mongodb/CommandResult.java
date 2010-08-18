// CommandResult.java

package com.mongodb;

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

    static class CommandFailure extends MongoException {
        CommandFailure( CommandResult res , String msg ){
            super( ServerError.getCode( res ) , msg );
        }
    }
}
