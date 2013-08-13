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