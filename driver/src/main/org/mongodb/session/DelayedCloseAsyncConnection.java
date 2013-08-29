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

package org.mongodb.session;

import org.bson.ByteBuf;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.ChannelReceiveArgs;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.SingleResultCallback;

import java.util.List;

import static org.mongodb.assertions.Assertions.isTrue;

class DelayedCloseAsyncConnection implements AsyncServerConnection {
    private boolean isClosed;
    private AsyncServerConnection wrapped;

    public DelayedCloseAsyncConnection(final AsyncServerConnection asyncConnection) {
        wrapped = asyncConnection;
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
        isTrue("open", !isClosed());
        wrapped.sendMessage(byteBuffers, callback);
    }

    @Override
    public void receiveMessage(final ChannelReceiveArgs channelReceiveArgs, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("open", !isClosed());
        wrapped.receiveMessage(channelReceiveArgs, callback);
    }

    @Override
    public ServerDescription getDescription() {
        isTrue("open", !isClosed());
        return wrapped.getDescription();
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public ServerAddress getServerAddress() {
        isTrue("open", !isClosed());
        return wrapped.getServerAddress();
    }
}
