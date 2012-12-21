package org.mongodb;

import static org.mongodb.OrderBy.ASC;

public class Index {
    private final String key;
    private final OrderBy orderBy;
    private final boolean unique;

    public Index(final String key) {
        this(key, ASC, false);
    }

    public Index(final String key, final OrderBy orderBy) {
        this(key, orderBy, false);
    }

    public Index(final String key, final OrderBy orderBy, final boolean unique) {
        this.key = key;
        this.orderBy = orderBy;
        this.unique = unique;
    }

    public String getKey() {
        return key;
    }

    public OrderBy getOrderBy() {
        return orderBy;
    }

    public boolean isUnique() {
        return unique;
    }
}
