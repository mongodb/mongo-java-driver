/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb;

import org.bson.types.Document;

import static org.mongodb.OrderBy.ASC;

/**
 * Represents an index to create on the database.  Used as an argument in ensureIndex
 */
public class Index implements ConvertibleToDocument {
    private final boolean unique;
    private final Document keys = new Document();
    private final String name;

    public Index(final Key... keys) {
        for (final Key key : keys) {
            addKey(key);
        }
        unique = false;
        name = generateIndexName();
    }

    public Index(final String... fields) {
        for (String field : fields) {
            addKey(field, ASC);
        }
        unique = false;
        name = generateIndexName();
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
        this.name = generateIndexName();
    }

    public boolean isUnique() {
        return unique;
    }

    public String getName() {
        return name;
    }

    @Override
    public Document toDocument() {
        return keys;
    }

    private void addKey(final Key key) {
        keys.append(key.fieldName, key.orderBy.getIntRepresentation());
    }

    private void addKey(final String fieldName, final OrderBy orderBy) {
        keys.append(fieldName, orderBy.getIntRepresentation());
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

    /**
     * Contains the pair that is the field name and the ordering value for each key of an index
     */
    public static class Key {
        private final String fieldName;
        private final OrderBy orderBy;

        public Key(final String fieldName, final OrderBy orderBy) {
            this.fieldName = fieldName;
            this.orderBy = orderBy;
        }
    }
}
