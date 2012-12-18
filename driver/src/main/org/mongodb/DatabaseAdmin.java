package org.mongodb;

/**
 * The administrative commands that can be run against a selected database.
 */
public interface DatabaseAdmin {
    /**
     * Drops this database.
     * @see <a href="http://docs.mongodb.org/manual/reference/commands/#dropDatabase">Drop database</a>
     */
    void drop();
}
