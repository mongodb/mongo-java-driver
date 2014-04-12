package org.mongodb.async;

import org.bson.types.ObjectId;
import org.mongodb.Document;

public class TargetDocument {
    private ObjectId id;
    private String x;

    public TargetDocument(final Document document) {
        this((ObjectId) document.get("_id"), document.get("x").toString());
    }

    public TargetDocument(final ObjectId id, final String x) {
        this.id = id;
        this.x = x;
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(final ObjectId id) {
        this.id = id;
    }

    public String getX() {
        return x;
    }

    public void setX(final String x) {
        this.x = x;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TargetDocument that = (TargetDocument) o;

        if (!id.equals(that.id)) {
            return false;
        }
        if (!x.equals(that.x)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + x.hashCode();
        return result;
    }
}
