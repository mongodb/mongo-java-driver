package com.mongodb;

/**
 * Subclass of {@link MongoException} representing a cursor-not-found exception.
 */
public class MongoCursorNotFoundException extends MongoException {

    private static final long serialVersionUID = -4415279469780082174L;

    private final long cursorId;
    private final ServerAddress serverAddress;

    /**
     * @param cursorId      cursor
     * @param serverAddress server address
     */
    MongoCursorNotFoundException(final long cursorId, final ServerAddress serverAddress) {
        super(-5, "Cursor " + cursorId + " not found on server " + serverAddress);
        this.cursorId = cursorId;
        this.serverAddress = serverAddress;
    }

    /**
     * Get the cursor id that wasn't found.
     *
     * @return the ID of the cursor
     */
    public long getCursorId() {
        return cursorId;
    }

    /**
     * The server address where the cursor is.
     *
     * @return the ServerAddress representing the server the cursor was on.
     */
    public ServerAddress getServerAddress() {
        return serverAddress;
    }
}
