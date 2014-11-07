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

import org.bson.LazyBSONCallback;
import org.bson.LazyBSONObject;
import org.bson.io.BSONByteBuffer;
import org.bson.util.annotations.Immutable;

/**
 * An immutable {@code DBObject} backed by a byte buffer that lazily provides keys and values on request. This is useful for transferring
 * BSON documents between servers when you don't want to pay the performance penalty of encoding or decoding them fully.
 */
@Immutable
public class LazyDBObject extends LazyBSONObject implements DBObject {

    /**
     * Construct an instance.
     *
     * @param bytes the raw bytes
     * @param callback the callback to use to construct nested values
     */
    public LazyDBObject(byte[] bytes, LazyBSONCallback callback){
        this(bytes, 0, callback);
    }

    /**
     * Construct an instance.
     *
     * @param bytes the raw bytes
     * @param offset the offset into the raw bytes
     * @param callback the callback to use to construct nested values
     */
    public LazyDBObject(byte[] bytes, int offset, LazyBSONCallback callback){
        super(bytes, offset, callback);
    }

    /**
     * @deprecated use {@link #LazyDBObject(byte[], org.bson.LazyBSONCallback)} instead
     */
    @Deprecated
    public LazyDBObject(BSONByteBuffer buff, LazyBSONCallback cbk){
        super(buff, cbk);
    }

    /**
     * @deprecated use {@link #LazyDBObject(byte[], int, org.bson.LazyBSONCallback)} instead
     */
    @Deprecated
    public LazyDBObject(BSONByteBuffer buff, int offset, LazyBSONCallback cbk){
        super(buff, offset, cbk);
    }

    @Override
    public void markAsPartialObject() {
        _partial = true;
    }

    @Override
    public boolean isPartialObject() {
        return _partial;
    }

    private boolean _partial = false;
}
