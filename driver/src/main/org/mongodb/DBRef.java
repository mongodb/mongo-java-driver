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

public class DBRef {
    private final Object id;
    private final String ref;

    public DBRef(final Object id, final String namespace) {
        this.id = id;
        ref = namespace;
    }

    @Override
    public String toString() {
        return "{ \"$ref\" : \"" + ref + "\", \"$id\" : \"" + id + "\" }";
    }

    /**
     * Gets the object's id
     *
     * @return
     */
    public Object getId() {
        return id;
    }

    /**
     * Gets the object's namespace (collection name)
     *
     * @return
     */
    public String getRef() {
        return ref;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final DBRef dbRefBase = (DBRef) o;

        if (id != null ? !id.equals(dbRefBase.id) : dbRefBase.id != null) {
            return false;
        }
        if (ref != null ? !ref.equals(dbRefBase.ref) : dbRefBase.ref != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (ref != null ? ref.hashCode() : 0);
        return result;
    }
}
