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

package com.mongodb.connection;

import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatStartedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerMonitorListener;

class NoOpServerMonitorListener implements ServerMonitorListener {
    @Override
    public void serverHearbeatStarted(final ServerHeartbeatStartedEvent event) {
    }

    @Override
    public void serverHeartbeatSucceeded(final ServerHeartbeatSucceededEvent event) {
    }

    @Override
    public void serverHeartbeatFailed(final ServerHeartbeatFailedEvent event) {
    }
}
