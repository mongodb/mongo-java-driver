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

package com.mongodb;

import com.mongodb.annotations.Immutable;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * A MongoDB namespace, which includes a database name and collection name.
 *
 * @since 3.0
 */
@Immutable
public final class MongoNamespace {
    public static final String COMMAND_COLLECTION_NAME = "$cmd";

    private final String databaseName;
    private final String collectionName;

    /**
     * Construct an instance.
     *
     * @param fullName the full namespace
     */
    public MongoNamespace(final String fullName) {
        notNull("fullName", fullName);
        isTrueArgument("fullName is of form <db>.<collection>", isFullNameValid(fullName));


        this.databaseName = getDatatabaseNameFromFullName(fullName);
        this.collectionName = getCollectionNameFullName(fullName);
    }

    /**
     * Construct an instance.
     *
     * @param databaseName the non-null database name
     * @param collectionName the non-null collection name
     */
    public MongoNamespace(final String databaseName, final String collectionName) {
        this.databaseName = notNull("databaseName", databaseName);
        this.collectionName = notNull("collectionName", collectionName);
    }

    /**
     * Gets the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Gets the collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Gets the full name, which is the database name and the collection name, separated by a period.
     *
     * @return the full name
     */
    public String getFullName() {
        return getDatabaseName() + "." + getCollectionName();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MongoNamespace that = (MongoNamespace) o;

        if (!collectionName.equals(that.collectionName)) {
            return false;
        }
        if (!databaseName.equals(that.databaseName)) {
            return false;
        }

        return true;
    }

    /**
     * Returns the standard MongoDB representation of a namespace, which is {@code &lt;database&gt;.&lt;collection&gt;}.
     *
     * @return string representation of the namespace.
     */
    @Override
    public String toString() {
        return databaseName + "." + collectionName;
    }

    @Override
    public int hashCode() {
        int result = databaseName.hashCode();
        result = 31 * result + (collectionName.hashCode());
        return result;
    }

    private static boolean isFullNameValid(final String fullName) {
        int firstDotIndex = fullName.indexOf(".");

        if (firstDotIndex == -1) {
            return false;
        }
        if (firstDotIndex == 0) {
            return false;
        }
        if (fullName.charAt(fullName.length() - 1) == '.') {
            return false;
        }
        if (fullName.charAt(firstDotIndex + 1) == '.') {
            return false;
        }
        return true;
    }

    private static String getCollectionNameFullName(final String namespace) {
        return namespace.substring(namespace.indexOf('.') + 1);
    }

    private static String getDatatabaseNameFromFullName(final String namespace) {
        return namespace.substring(0, namespace.indexOf('.'));
    }
}
