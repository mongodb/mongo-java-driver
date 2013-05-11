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

package org.mongodb.impl;

import org.mongodb.async.SingleResultCallback;
import org.mongodb.io.ChannelAwareOutputBuffer;
import org.mongodb.io.ResponseBuffers;

import static org.mongodb.assertions.Assertions.isTrue;

class DelayedCloseMongoAsyncConnection extends DelayedCloseMongoConnection implements MongoAsyncConnection {
    private MongoAsyncConnection wrapped;

    public DelayedCloseMongoAsyncConnection(final MongoAsyncConnection asyncConnection) {
        wrapped = asyncConnection;
    }

    @Override
    public void sendMessage(final ChannelAwareOutputBuffer buffer, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("open", !isClosed());
        wrapped.sendMessage(buffer, callback);
    }

    @Override
    public void sendAndReceiveMessage(final ChannelAwareOutputBuffer buffer, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("open", !isClosed());
        wrapped.sendAndReceiveMessage(buffer, callback);
    }

    @Override
    public void receiveMessage(final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("open", !isClosed());
        wrapped.receiveMessage(callback);
    }

    @Override
    protected MongoConnection getWrapped() {
        return wrapped;
    }
}
