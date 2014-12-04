package com.mongodb;

/**
 * An exception indicating that a command sent to a MongoDB server returned a failure.
 *
 * @since 2.13
 */
public class MongoCommandException extends MongoException {
    private static final long serialVersionUID = 8160676451944215078L;
    private final CommandResult commandResult;

    public MongoCommandException(final CommandResult commandResult) {
        super(ServerError.getCode(commandResult), commandResult.toString());
        this.commandResult = commandResult;
    }

    /**
     * Gets the address of the server that this command failed on.
     * @return the server address
     */
    public ServerAddress getServerAddress() {
        return commandResult.getServerUsed();
    }

    /**
     * Gets the error code associated with the command failure.
     *
     * @return the error code
     */
    public int getErrorCode() {
        return getCode();
    }

    /**
     * Gets the error message associated with the command failure.
     *
     * @return the error message
     */
    public String getErrorMessage() {
        return commandResult.getErrorMessage();
    }

    CommandResult getCommandResult() {
        return commandResult;
    }
}
