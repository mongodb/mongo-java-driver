package org.mongodb;

/**
 * All the commands that can be run without needing a specific database.
 */
public interface ClientAdmin {
    /**
     * @return a non-null number if the server is reachable.
     */
    //TODO: it's not clear from the documentation what the return type should be
    //http://docs.mongodb.org/manual/reference/command/ping/
    double ping();
}
