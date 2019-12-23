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

package com.mongodb;

import org.bson.LazyBSONCallback;
import org.bson.LazyBSONList;

/**
 * A {@code LazyDBObject} representing a BSON array.
 */
public class LazyDBList extends LazyBSONList implements DBObject {

    private boolean isPartial;

    /**
     * Construct an instance with the given raw bytes and offset.
     *
     * @param bytes the raw BSON bytes
     * @param callback the callback to use to create nested values
     */
    public LazyDBList(final byte[] bytes, final LazyBSONCallback callback) {
        super(bytes, callback);
    }

    /**
     * Construct an instance with the given raw bytes and offset.
     *
     * @param bytes the raw BSON bytes
     * @param offset the offset into the raw bytes
     * @param callback the callback to use to create nested values
     */
    public LazyDBList(final byte[] bytes, final int offset, final LazyBSONCallback callback) {
        super(bytes, offset, callback);
    }

    @Override
    public void markAsPartialObject() {
        isPartial = true;
    }

    @Override
    public boolean isPartialObject() {
        return isPartial;
    }
}
