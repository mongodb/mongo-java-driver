package com.mongodb;

import org.bson.util.annotations.Immutable;

import static org.bson.util.Assertions.notNull;

/**
 * A replica set tag.
 *
 * @since 2.13
 * @mongodb.driver.manual tutorial/configure-replica-set-tag-sets Tag Sets
 */
@Immutable
public final class Tag {
    private final String name;
    private final String value;

    /**
     * Construct a new instance.
     *
     * @param name the tag name
     * @param value the value of the tag
     */
    public Tag(final String name, final String value) {
        this.name = notNull("name", name);
        this.value = notNull("value", value);
    }

    /**
     * Gets the name of the replica set tag.
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the value of the replica set tag.
     * @return the value
     */
    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Tag that = (Tag) o;

        if (!name.equals(that.name)) {
            return false;
        }
        if (!value.equals(that.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Tag{"
               + "name='" + name + '\''
               + ", value='" + value + '\''
               + '}';
    }
}
