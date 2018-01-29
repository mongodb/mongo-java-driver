/*
 * Copyright 2008-present MongoDB, Inc.
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

package org.bson;

import org.bson.types.ObjectId;

/**
 * Holder for a BSON type DBPointer(0x0c). It's deprecated in BSON Specification and present here because of compatibility reasons.
 *
 * @since 3.0
 */
public class BsonDbPointer extends BsonValue {

    private final String namespace;
    private final ObjectId id;

    /**
     * Construct a new instance with the given namespace and id.
     *
     * @param namespace the namespace
     * @param id the id
     */
    public BsonDbPointer(final String namespace, final ObjectId id) {
        if (namespace == null) {
            throw new IllegalArgumentException("namespace can not be null");
        }
        if (id == null) {
            throw new IllegalArgumentException("id can not be null");
        }
        this.namespace = namespace;
        this.id = id;
    }

    @Override
    public BsonType getBsonType() {
        return BsonType.DB_POINTER;
    }

    /**
     * Gets the namespace.
     *
     * @return the namespace
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public ObjectId getId() {
        return id;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BsonDbPointer dbPointer = (BsonDbPointer) o;

        if (!id.equals(dbPointer.id)) {
            return false;
        }
        if (!namespace.equals(dbPointer.namespace)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = namespace.hashCode();
        result = 31 * result + id.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "BsonDbPointer{"
               + "namespace='" + namespace + '\''
               + ", id=" + id
               + '}';
    }
}
