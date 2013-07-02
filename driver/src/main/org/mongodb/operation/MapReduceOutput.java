package org.mongodb.operation;

/**
 * A class that represents 'out' argument for a map/reduce operation.
 */
public class MapReduceOutput {

    private String collectionName;
    private Action action;
    private String databaseName;
    private boolean sharded;
    private boolean nonAtomic;


    /**
     * Constructs a new instance of the {@code MapReduceOutput}.
     *
     * @param collectionName the name of the collection that you want the map-reduce operation to write its output.
     */
    public MapReduceOutput(final String collectionName) {
        this.collectionName = collectionName;
        this.action = Action.REPLACE;
    }

    //CHECKSTYLE:OFF

    /**
     * Specify the name of the database that you want the map-reduce operation to write its output.
     *
     * @param databaseName the name of the database.
     * @return the same {@code MapReduceOutput} instance as used for the method invocation for chaining
     */
    public MapReduceOutput database(final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    /**
     * Specify the {@code Action} to be used when writing to a collection that already exists.
     *
     * @param action
     * @return the same {@code MapReduceOutput} instance as used for the method invocation for chaining
     */
    public MapReduceOutput action(final Action action) {
        this.action = action;
        return this;
    }
    //CHECKSTYLE:ON

    /**
     * Add a 'sharded' flag.
     * <p/>
     * If speficied and you have enabled sharding on output database, the map-reduce operation will
     * shard the output collection using the _id field as the shard key.
     *
     * @return the same {@code MapReduceOutput} instance as used for the method invocation for chaining
     */
    public MapReduceOutput sharded() {
        this.sharded = true;
        return this;
    }


    /**
     * Add a 'nonAtomic' flag. Valid only together with {@code Action.MERGE} and {@code Action.REDUCE}
     * <p/>
     * If specified the post-processing step will prevent MongoDB from locking the database;
     * however, other clients will be able to read intermediate states of the output collection.
     * Otherwise the map reduce operation must lock the database during post-processing.
     *
     * @return the same {@code MapReduceOutput} instance as used for the method invocation for chaining
     */
    public MapReduceOutput nonAtomic() {
        this.nonAtomic = true;
        return this;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public Action getAction() {
        return action;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public boolean isSharded() {
        return sharded;
    }

    public boolean isNonAtomic() {
        return nonAtomic;
    }

    public static enum Action {
        /**
         * Replace the contents of the <collectionName> if the collection with the <collectionName> exists.
         */
        REPLACE("replace"),

        /**
         * Merge the new result with the existing result if the output collection already exists.
         * If an existing document has the same key as the new result, overwrite that existing document.
         */
        MERGE("merge"),

        /**
         * Merge the new result with the existing result if the output collection already exists.
         * If an existing document has the same key as the new result, apply the reduce function
         * to both the new and the existing documents and overwrite the existing document with the result.
         */
        REDUCE("reduce");

        private String value;

        private Action(final String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
