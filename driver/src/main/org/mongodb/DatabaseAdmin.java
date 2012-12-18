package org.mongodb;

import java.util.Set;

/**
 * The administrative commands that can be run against a selected database.
 */
public interface DatabaseAdmin {
    /**
     * Drops this database.
     *
     * @see <a href="http://docs.mongodb.org/manual/reference/commands/#dropDatabase">Drop database</a>
     */
    void drop();

    /**
     * @return a Set of the names of all the collections in this database
     */
    Set<String> getCollectionNames();
}
