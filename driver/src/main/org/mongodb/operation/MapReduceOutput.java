package org.mongodb.operation;

public class MapReduceOutput {

    private String collectionName;
    private Action action;
    private String databaseName;
    private boolean sharded;
    private boolean nonAtomic;


    public MapReduceOutput(final String collectionName) {
        this.collectionName = collectionName;
        this.action = Action.REPLACE;
    }

    //CHECKSTYLE:OFF TODO: http://checkstyle.sourceforge.net/config_coding.html#HiddenField needs to be supressed?
    public MapReduceOutput database(final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public MapReduceOutput action(final Action action) {
        this.action = action;
        return this;
    }
    //CHECKSTYLE:ON

    public MapReduceOutput sharded() {
        this.sharded = true;
        return this;
    }

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
        REPLACE("replace"),
        MERGE("merge"),
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
