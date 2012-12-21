package org.mongodb;

import org.bson.types.Document;

import static org.mongodb.OrderBy.ASC;

public class Index {
    private final boolean unique;
    private final Document keys = new Document();

    private String name;

    //TODO: now this object isn't immutable
    //add builder?
    public Index() {
        unique = false;
    }

    public Index(final String key) {
        this(key, ASC, false);
    }

    public Index(final String key, final OrderBy orderBy) {
        this(key, orderBy, false);
    }

    public Index(final String key, final OrderBy orderBy, final boolean unique) {
        addKey(key, orderBy);
        this.unique = unique;
    }

    public boolean isUnique() {
        return unique;
    }

    public void addKey(final String fieldName) {
        addKey(fieldName, ASC);
    }

    public void addKey(final String fieldName, final OrderBy orderBy) {
        keys.append(fieldName, orderBy.getIntRepresentation());
    }

    public Document getAsDocument() {
        return keys;
    }

    public String getName() {
        if (name == null) {
            name = generateIndexName();
        }
        return name;
    }

    /**
     * Convenience method to generate an index name from the set of fields it is over.
     *
     * @return a string representation of this index's fields
     */
    private String generateIndexName() {
        final StringBuilder indexName = new StringBuilder();
        for (String keyNames : this.keys.keySet()) {
            if (indexName.length() != 0) {
                indexName.append('_');
            }
            indexName.append(keyNames).append('_');
            //is this ever anything other than an int?
            final Object ascOrDescValue = this.keys.get(keyNames);
            if (ascOrDescValue instanceof Number || ascOrDescValue instanceof String) {
                indexName.append(ascOrDescValue.toString().replace(' ', '_'));
            }
        }
        return indexName.toString();
    }
}
