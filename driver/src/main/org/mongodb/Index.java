/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
    private final String name;
    private final Document keys = new Document();

    public Index(final Key<?>... keys) {
        for (final Key<?> key : keys) {
            addKey(key);
        }
        unique = false;
        name = generateIndexName();
    }

    public Index(final String name, final Key<?>... keys) {
        this(name, false, keys);
    }

    public Index(final String name, final boolean unique, final Key<?>... keys) {
        for (final Key<?> key : keys) {
            addKey(key);
        }
        this.unique = unique;
        this.name = name != null ? name : generateIndexName();
    }

    public Index(final String... keyNames) {
        for (final String key : keyNames) {
            addKey(key, ASC);
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

    public String getName() {
        return name;
    }

    @Override
    public Document toDocument() {
        final Document indexDetails = new Document();
        indexDetails.append("name", name);
        indexDetails.append("key", keys);
        indexDetails.append("unique", unique);

        return indexDetails;
    }

    private void addKey(final Key<?> key) {
        keys.append(key.getFieldName(), key.getValue());
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
        for (final String keyNames : this.keys.keySet()) {
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
    public static class OrderedKey implements Key<Integer> {
        private final String fieldName;
        private final OrderBy orderBy;

        public OrderedKey(final String fieldName, final OrderBy orderBy) {
            this.fieldName = fieldName;
            this.orderBy = orderBy;
        }

        @Override
        public String getFieldName() {
            return fieldName;
        }

        @Override
        public Integer getValue() {
            return orderBy.getIntRepresentation();
        }
    }

    public static class GeoKey implements Key<String> {
        private final String fieldName;

        public GeoKey(final String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public String getFieldName() {
            return fieldName;
        }

        @Override
        public String getValue() {
            return "2d";
        }
    }

    public interface Key<T> {
        String getFieldName();

        T getValue();
    }
}
