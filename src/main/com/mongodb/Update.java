package com.mongodb;

class Update  {
    private final DBObject updateOperations;
    private final DBObject filter;
    private boolean isUpsert = false;
    private boolean isMulti = false;

    public Update(final DBObject filter, final DBObject updateOperations) {
        this.filter = filter;
        this.updateOperations = updateOperations;
    }

    DBObject getFilter() {
        return filter;
    }

    public DBObject getUpdateOperations() {
        return updateOperations;
    }

    public boolean isMulti() {
        return isMulti;
    }

    boolean isUpsert() {
        return isUpsert;
    }

    public Update multi(final boolean isMulti) {
        this.isMulti = isMulti;
        return this;
    }

    public Update upsert(final boolean isUpsert) {
        this.isUpsert = isUpsert;
        return this;
    }
}
