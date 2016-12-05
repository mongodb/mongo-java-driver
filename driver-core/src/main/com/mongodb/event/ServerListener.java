/*
 * Copyright 2016 MongoDB, Inc.
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
 *
 */

package com.mongodb.event;

import java.util.EventListener;

/**
 * A listener for server-related events
 *
 * @since 3.3
 */
public interface ServerListener extends EventListener {

    /**
     *  Listener for server opening events.
     *
     * @param event the server opening event
     */
    void serverOpening(ServerOpeningEvent event);

    /**
     * Listener for server closed events.
     *
     * @param event the server closed event
     */
    void serverClosed(ServerClosedEvent event);

    /**
     * Listener for server description changed events.
     *
     * @param event the server description changed event
     */
    void serverDescriptionChanged(ServerDescriptionChangedEvent event);
}
