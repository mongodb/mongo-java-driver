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

import java.util.List;

/**
 * A {@code BSONCallback} for creation of {@code LazyBSONObject} and {@code LazyBSONList} instances.
 */
public class LazyBSONCallback extends EmptyBSONCallback {
    private Object root;

    @Override
    public void reset() {
        this.root = null;
    }

    @Override
    public Object get() {
        return getRoot();
    }

    @Override
    public void gotBinary(final String name, final byte type, final byte[] data) {
        setRoot(createObject(data, 0));
    }

    /**
     * Create a {@code LazyBSONObject} instance from the given bytes starting from the given offset.
     *
     * @param bytes the raw BSON bytes
     * @param offset the offset into the bytes
     * @return the LazyBSONObject
     */
    public Object createObject(final byte[] bytes, final int offset) {
        return new LazyBSONObject(bytes, offset, this);
    }

    /**
     * Create a {@code LazyBSONList} from the given bytes starting from the given offset.
     *
     * @param bytes the raw BSON bytes
     * @param offset the offset into the bytes
     * @return the LazyBSONList
     */
    @SuppressWarnings("rawtypes")
    public List createArray(final byte[] bytes, final int offset) {
        return new LazyBSONList(bytes, offset, this);
    }

    /**
     * This is a factory method pattern to create appropriate objects for BSON type DBPointer(0x0c).
     *
     * @param ns the namespace of the reference
     * @param id the identifier of the reference
     * @return object to be used as reference representation
     */
    public Object createDBRef(final String ns, final ObjectId id) {
        return new BasicBSONObject("$ns", ns).append("$id", id);
    }

    private Object getRoot() {
        return root;
    }

    private void setRoot(final Object root) {
        this.root = root;
    }
}
