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

import org.mongodb.annotations.NotThreadSafe;
import org.mongodb.connection.BaseConnection;
import org.mongodb.connection.ChannelAwareOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ResponseSettings;
import org.mongodb.connection.ServerConnection;
import org.mongodb.connection.ServerDescription;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

@NotThreadSafe
class DelayedCloseConnection extends DelayedCloseBaseConnection implements ServerConnection {
    private ServerConnection wrapped;

    public DelayedCloseConnection(final ServerConnection wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }

    @Override
    public void sendMessage(final ChannelAwareOutputBuffer buffer) {
        isTrue("open", !isClosed());
        wrapped.sendMessage(buffer);
    }

    @Override
    public ResponseBuffers receiveMessage(final ResponseSettings responseSettings) {
        isTrue("open", !isClosed());
        return wrapped.receiveMessage(responseSettings);
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
