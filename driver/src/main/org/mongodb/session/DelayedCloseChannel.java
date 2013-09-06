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
import org.mongodb.annotations.NotThreadSafe;
import org.mongodb.connection.Channel;
import org.mongodb.connection.ChannelReceiveArgs;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.SingleResultCallback;

import java.util.List;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

@NotThreadSafe
class DelayedCloseChannel implements Channel {
    private Channel wrapped;
    private boolean isClosed;

    public DelayedCloseChannel(final Channel wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers) {
        isTrue("open", !isClosed());
        wrapped.sendMessage(byteBuffers);
    }

    @Override
    public ResponseBuffers receiveMessage(final ChannelReceiveArgs channelReceiveArgs) {
        isTrue("open", !isClosed());
        return wrapped.receiveMessage(channelReceiveArgs);
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
        isTrue("open", !isClosed());
        wrapped.sendMessageAsync(byteBuffers, callback);
    }

    @Override
    public void receiveMessageAsync(final ChannelReceiveArgs channelReceiveArgs, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("open", !isClosed());
        wrapped.receiveMessageAsync(channelReceiveArgs, callback);
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

    @Override
    public String getId() {
        return wrapped.getId();
    }
}
