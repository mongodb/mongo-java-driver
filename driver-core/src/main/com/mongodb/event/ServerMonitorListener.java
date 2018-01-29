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


package com.mongodb.event;


import java.util.EventListener;

/**
 * A listener for server monitor-related events
 *
 * @since 3.3
 */
public interface ServerMonitorListener extends EventListener {

    /**
     * Listener for server heartbeat started events.
     *
     * @param event the server heartbeat started event
     */
    void serverHearbeatStarted(ServerHeartbeatStartedEvent event);

    /**
     * Listener for server heartbeat succeeded events.
     *
     * @param event the server heartbeat succeeded event
     */
    void serverHeartbeatSucceeded(ServerHeartbeatSucceededEvent event);

    /**
     * Listener for server heartbeat failed events.
     *
     * @param event the server heartbeat failed event
     */
    void serverHeartbeatFailed(ServerHeartbeatFailedEvent event);
}
