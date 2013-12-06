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


import java.util.List;

import static org.mongodb.OrderBy.ASC;


/**
 * Represents an index to create on the database.  Used as an argument in ensureIndex
 */
public final class Index implements ConvertibleToDocument {
    private final String name;
    /**
     * Ensures that the indexed key value is unique
     */
    private final boolean unique;

    /**
     * Tells the unique index to drop duplicates silently when creating; only the first will be kept
     */
    private final boolean dropDups;

    /**
     * Create the index in the background
     */
    private final boolean background;

    /**
     * Create the index with the sparse option
     */
    private final boolean sparse;

    /**
     * defines the time to live for documents in the collection
     */
    private final int expireAfterSeconds;

    private final Document keys;

    private final Document extra;

    private Index(final String name, final boolean unique, final boolean dropDups, final boolean sparse, final boolean background,
                  final int expireAfterSeconds, final Document keys, final Document extra) {
        this.name = name;
        this.unique = unique;
        this.dropDups = dropDups;
        this.sparse = sparse;

        this.background = background;
        this.expireAfterSeconds = expireAfterSeconds;
        this.keys = keys;
        this.extra = extra;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getName() {
        return name;
    }

    @Override
    public Document toDocument() {
        Document indexDetails = new Document();
        indexDetails.append("name", name);
        indexDetails.append("key", keys);
        if (unique) {
            indexDetails.append("unique", unique);
        }
        if (sparse) {
            indexDetails.append("sparse", sparse);
        }
        if (dropDups) {
            indexDetails.append("dropDups", dropDups);
        }
        if (background) {
            indexDetails.append("background", background);
        }
        if (expireAfterSeconds != -1) {
            indexDetails.append("expireAfterSeconds", expireAfterSeconds);
        }
        indexDetails.putAll(extra);

        return indexDetails;
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

    public static class Text implements Key<String> {
        private final String fieldName;

        public Text(final String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public String getFieldName() {
            return fieldName;
        }

        @Override
        public String getValue() {
            return "text";
        }
    }

    public interface Key<T> {
        String getFieldName();

        T getValue();
    }

    public static final class Builder {
        private String name;
        private boolean unique = false;
        private boolean dropDups = false;
        private boolean background = false;
        private boolean sparse = false;
        private int expireAfterSeconds = -1;
        private final Document keys = new Document();
        private final Document extra = new Document();

        private Builder() {
        }

        /**
         * Sets the name of the index.
         */
        public Builder name(final String indexName) {
            this.name = indexName;
            return this;
        }

        /**
         * Ensures that the indexed key value is unique
         */
        public Builder unique() {
            unique = true;
            return this;
        }

        public Builder unique(final boolean value) {
            this.unique = value;
            return this;
        }

        /**
         * Tells the unique index to drop duplicates silently when creating; only the first will be kept
         */
        public Builder dropDups() {
            dropDups = true;
            return this;
        }

        public Builder dropDups(final boolean value) {
            this.dropDups = value;
            return this;
        }

        /**
         * Create the index in the background
         */
        public Builder background() {
            background = true;
            return this;
        }

        public Builder background(final boolean value) {
            this.background = value;
            return this;
        }

        /**
         * Create the index with the sparse option
         */
        public Builder sparse() {
            sparse = true;
            return this;
        }

        public Builder sparse(final boolean value) {
            this.sparse = value;
            return this;
        }

        /**
         * Defines the time to live for documents in the collection
         */
        public Builder expireAfterSeconds(final int seconds) {
            expireAfterSeconds = seconds;
            return this;
        }

        public Builder addKey(final String key) {
            return addKey(key, ASC);
        }

        public Builder addKeys(final String... keyNames) {
            for (final String keyName : keyNames) {
                addKey(keyName);
            }
            return this;
        }

        public Builder addKey(final String key, final OrderBy orderBy) {
            keys.put(key, orderBy.getIntRepresentation());
            return this;
        }

        public Builder addKey(final Key<?> key) {
            keys.put(key.getFieldName(), key.getValue());
            return this;
        }

        public Builder addKeys(final Key<?>... newKeys) {
            for (final Key<?> key : newKeys) {
                addKey(key);
            }
            return this;
        }

        public Builder addKeys(final List<Key<?>> newKeys) {
            for (final Key<?> key : newKeys) {
                addKey(key);
            }
            return this;
        }

        public Builder extra(final String key, final Object value) {
            extra.put(key, value);
            return this;
        }

        /**
         * Convenience method to generate an index name from the set of fields it is over.
         *
         * @return a string representation of this index's fields
         */
        private String generateIndexName() {
            StringBuilder indexName = new StringBuilder();
            for (final String keyNames : this.keys.keySet()) {
                if (indexName.length() != 0) {
                    indexName.append('_');
                }
                indexName.append(keyNames).append('_');
                //is this ever anything other than an int?
                Object ascOrDescValue = this.keys.get(keyNames);
                if (ascOrDescValue instanceof Number || ascOrDescValue instanceof String) {
                    indexName.append(ascOrDescValue.toString().replace(' ', '_'));
                }
            }
            return indexName.toString();
        }

        public Index build() {
            if (name == null) {
                name = generateIndexName();
            }
            return new Index(name, unique, dropDups, sparse, background, expireAfterSeconds, keys, extra);
        }
    }
}
