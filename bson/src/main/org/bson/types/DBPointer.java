package org.bson.types;

/**
 * Holder for a BSON type DBPointer(0x0c). It's deprecated in BSON Specification and
 * present here because of compatibility reasons.
 */
public class DBPointer {
    private final String namespace;
    private final ObjectId id;

    public DBPointer(final String namespace, final ObjectId id) {
        this.namespace = namespace;
        this.id = id;
    }

    public String getNamespace() {
        return namespace;
    }

    public ObjectId getId() {
        return id;
    }
}
