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
import org.mongodb.connection.BaseConnection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ResponseSettings;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.SingleResultCallback;

import java.util.List;

import static org.mongodb.assertions.Assertions.isTrue;

class DelayedCloseAsyncConnection extends DelayedCloseBaseConnection implements AsyncServerConnection {
    private AsyncServerConnection wrapped;

    public DelayedCloseAsyncConnection(final AsyncServerConnection asyncConnection) {
        wrapped = asyncConnection;
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("open", !isClosed());
        wrapped.sendMessage(byteBuffers, callback);
    }

    @Override
    public void sendAndReceiveMessage(final List<ByteBuf> byteBuffers, final ResponseSettings responseSettings,
                                      final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("open", !isClosed());
        wrapped.sendAndReceiveMessage(byteBuffers, responseSettings, callback);
    }

    @Override
    public void receiveMessage(final ResponseSettings responseSettings, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("open", !isClosed());
        wrapped.receiveMessage(responseSettings, callback);
    }

    @Override
    protected BaseConnection getWrapped() {
        return wrapped;
    }

    @Override
    public ServerDescription getDescription() {
        isTrue("open", !isClosed());
        return wrapped.getDescription();
    }
}
