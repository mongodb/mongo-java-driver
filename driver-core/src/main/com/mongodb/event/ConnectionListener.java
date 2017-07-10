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

package com.mongodb.event;

import com.mongodb.annotations.Beta;

import java.util.EventListener;

/**
 * A listener for connection-related events.
 *
 * @deprecated - No longer used
 */
@Beta
@Deprecated
public interface ConnectionListener extends EventListener {

    /**
     * Publish a connection opened event.
     *
     * @param event the event
     */
    void connectionOpened(ConnectionOpenedEvent event);

    /**
     * Publish a connection message closed event.
     *
     * @param event the event
     */
    void connectionClosed(ConnectionClosedEvent event);

    /**
     * Publish a connection messages sent event.
     *
     * @param event the event
     */
    void messagesSent(ConnectionMessagesSentEvent event);

    /**
     * Publish a connection message received event.
     *
     * @param event the event
     */
    void messageReceived(ConnectionMessageReceivedEvent event);
}
