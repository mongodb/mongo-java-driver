package com.mongodb;

/**
 * Container for the result of aggregation operation.
 */
public class AggregationOutput {
    protected final CommandResult commandResult;
    protected final DBObject command;

    /**
     * Create new container. This class should be hidden, so don't use it in your code.
     *
     * @param command       command, used to perform the operation
     * @param commandResult result of the operation
     */
    public AggregationOutput(final DBObject command, final CommandResult commandResult) {
        if (!commandResult.containsField("result") && !(command.get("result") instanceof Iterable)) {
            throw new IllegalArgumentException("Result undefined");
        }
        this.commandResult = commandResult;
        this.command = command;
    }

    /**
     * Returns the results of the aggregation.
     *
     * @return iterable collection of {@link DBObject}
     */
    @SuppressWarnings("unchecked")
    public Iterable<DBObject> results() {
        return (Iterable<DBObject>) commandResult.get("result");
    }

    /**
     * Returns the command result of the aggregation.
     *
     * @return aggregation command result
     */
    public CommandResult getCommandResult() {
        return commandResult;
    }

    /**
     * Returns the original aggregation command.
     *
     * @return a command document
     */
    public DBObject getCommand() {
        return command;
    }

    /**
     * Returns the address of the server used to execute the aggregation.
     *
     * @return address of the server
     */
    public ServerAddress getServerUsed() {
        return commandResult.getServerUsed();
    }

    @Override
    public String toString() {
        return commandResult.toString();
    }
}
