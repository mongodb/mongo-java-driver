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

import com.mongodb.annotations.Immutable;
import org.bson.LazyBSONCallback;
import org.bson.LazyBSONObject;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.StringWriter;

/**
 * An immutable {@code DBObject} backed by a byte buffer that lazily provides keys and values on request. This is useful for transferring
 * BSON documents between servers when you don't want to pay the performance penalty of encoding or decoding them fully.
 */
@Immutable
public class LazyDBObject extends LazyBSONObject implements DBObject {

    private boolean isPartial = false;

    /**
     * Construct an instance.
     *
     * @param bytes the raw bytes
     * @param callback the callback to use to construct nested values
     */
    public LazyDBObject(final byte[] bytes, final LazyBSONCallback callback) {
        super(bytes, callback);
    }

    /**
     * Construct an instance.
     *
     * @param bytes the raw bytes
     * @param offset the offset into the raw bytes
     * @param callback the callback to use to construct nested values
     */
    public LazyDBObject(final byte[] bytes, final int offset, final LazyBSONCallback callback) {
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

    /**
     * Returns a JSON serialization of this object
     *
     * @return JSON serialization
     */
    public String toString() {
        JsonWriter writer = new JsonWriter(new StringWriter(), JsonWriterSettings.builder().build());
        DBObjectCodec.getDefaultRegistry().get(LazyDBObject.class).encode(writer, this,
                EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        return writer.getWriter().toString();
    }
}
