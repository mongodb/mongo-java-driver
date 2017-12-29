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

package com.mongodb.connection;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.event.CommandListener;
import org.bson.io.OutputBuffer;

import static com.mongodb.connection.ProtocolHelper.sendCommandFailedEvent;

class SendMessageCallback<T> implements SingleResultCallback<Void> {
    private final OutputBuffer buffer;
    private final InternalConnection connection;
    private final SingleResultCallback<ResponseBuffers> receiveMessageCallback;
    private final int requestId;
    private final RequestMessage message;
    private final CommandListener commandListener;
    private final long startTimeNanos;
    private final SingleResultCallback<T> callback;
    private final String commandName;

    SendMessageCallback(final InternalConnection connection, final OutputBuffer buffer, final RequestMessage message,
                        final String commandName, final long startTimeNanos, final CommandListener commandListener,
                        final SingleResultCallback<T> callback, final SingleResultCallback<ResponseBuffers> receiveMessageCallback) {
        this(connection, buffer, message, message.getId(), commandName, startTimeNanos, commandListener, callback, receiveMessageCallback);
    }

    SendMessageCallback(final InternalConnection connection, final OutputBuffer buffer, final RequestMessage message,
                        final int requestId, final String commandName, final long startTimeNanos, final CommandListener commandListener,
                        final SingleResultCallback<T> callback, final SingleResultCallback<ResponseBuffers> receiveMessageCallback) {
        this.buffer = buffer;
        this.connection = connection;
        this.message = message;
        this.commandName = commandName;
        this.commandListener = commandListener;
        this.startTimeNanos = startTimeNanos;
        this.callback = callback;
        this.receiveMessageCallback = receiveMessageCallback;
        this.requestId = requestId;
    }

    @Override
    public void onResult(final Void result, final Throwable t) {
        buffer.close();
        if (t != null) {
            if (commandListener != null){
                sendCommandFailedEvent(message, commandName, connection.getDescription(), System.nanoTime() - startTimeNanos, t,
                        commandListener);
            }
            callback.onResult(null, t);
        } else {
            connection.receiveMessageAsync(requestId, receiveMessageCallback);
        }
    }
}
