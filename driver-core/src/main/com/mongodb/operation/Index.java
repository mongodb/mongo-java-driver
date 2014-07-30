/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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


package com.mongodb.operation;


import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.OrderBy.ASC;


/**
 * Represents an index to create on the database.  Used as an argument in createIndexes
 *
 * @since 3.0
 */
public final class Index {
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

    private final BsonDocument keys;

    private final BsonDocument extra;

    private Index(final String name, final boolean unique, final boolean dropDups, final boolean sparse, final boolean background,
                  final int expireAfterSeconds, final BsonDocument keys, final BsonDocument extra) {
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

    public boolean isUnique() {
        return unique;
    }

    public boolean isDropDups() {
        return dropDups;
    }

    public boolean isBackground() {
        return background;
    }

    public boolean isSparse() {
        return sparse;
    }

    public int getExpireAfterSeconds() {
        return expireAfterSeconds;
    }

    public BsonDocument getKeys() {
        return keys;
    }

    public BsonDocument getExtra() {
        return extra;
    }

    /**
     * Contains the pair that is the field name and the ordering value for each key of an index
     */
    public static class OrderedKey implements Key<BsonInt32> {
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
        public BsonInt32 getValue() {
            return new BsonInt32(orderBy.getIntRepresentation());
        }
    }

    public static class GeoKey implements Key<BsonString> {
        private final String fieldName;

        public GeoKey(final String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public String getFieldName() {
            return fieldName;
        }

        @Override
        public BsonString getValue() {
            return new BsonString("2d");
        }
    }

    public static class GeoSphereKey implements Key<BsonString> {
        private final String fieldName;

        public GeoSphereKey(final String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public String getFieldName() {
            return fieldName;
        }

        @Override
        public BsonString getValue() {
            return new BsonString("2dsphere");
        }
    }

    public static class Text implements Key<BsonString> {
        private final String fieldName;

        public Text(final String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public String getFieldName() {
            return fieldName;
        }

        @Override
        public BsonString getValue() {
            return new BsonString("text");
        }
    }

    public interface Key<T extends BsonValue> {
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
        private final BsonDocument keys = new BsonDocument();
        private BsonDocument extra = new BsonDocument();

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
            keys.put(key, new BsonInt32(orderBy.getIntRepresentation()));
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

        public Builder extra(final BsonDocument extra) {
            this.extra = notNull("extra", extra);
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
                BsonValue ascOrDescValue = this.keys.get(keyNames);
                if (ascOrDescValue instanceof BsonInt32) {
                    indexName.append(((BsonInt32) ascOrDescValue).getValue());
                } else if (ascOrDescValue instanceof BsonString) {
                    indexName.append(((BsonString) ascOrDescValue).getValue().replace(' ', '_'));
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
