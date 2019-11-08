/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.internal.async.client.gridfs.AsyncGridFSUploadStream;
import com.mongodb.reactivestreams.client.gridfs.GridFSUploadStream;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;

import static com.mongodb.assertions.Assertions.notNull;


final class GridFSUploadStreamImpl implements GridFSUploadStream {

    private final AsyncGridFSUploadStream wrapped;

    GridFSUploadStreamImpl(final AsyncGridFSUploadStream wrapped) {
        this.wrapped = notNull("GridFSUploadStream", wrapped);
    }

    @Override
    public ObjectId getObjectId() {
        return wrapped.getObjectId();
    }

    @Override
    public BsonValue getId() {
        return wrapped.getId();
    }

    @Override
    public Publisher<Integer> write(final ByteBuffer src) {
        return Publishers.publish(callback -> wrapped.write(src, callback));
    }

    @Override
    public Publisher<Void> close() {
        return Publishers.publish(wrapped::close);
    }

    @Override
    public Publisher<Void> abort() {
        return Publishers.publish(wrapped::abort);
    }
}
