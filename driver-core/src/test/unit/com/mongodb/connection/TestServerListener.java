/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.connection;

import com.mongodb.event.ServerClosedEvent;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerOpeningEvent;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

class TestServerListener implements ServerListener {
    private ServerOpeningEvent serverOpeningEvent;
    private ServerClosedEvent serverClosedEvent;
    private final List<ServerDescriptionChangedEvent> serverDescriptionChangedEvents = new ArrayList<ServerDescriptionChangedEvent>();

    @Override
    public void serverOpening(final ServerOpeningEvent event) {
        isTrue("serverOpeningEvent is null", serverOpeningEvent == null);
        serverOpeningEvent = event;
    }

    @Override
    public void serverClosed(final ServerClosedEvent event) {
        isTrue("serverClostedEvent is null", serverClosedEvent == null);
        serverClosedEvent = event;
    }

    @Override
    public void serverDescriptionChanged(final ServerDescriptionChangedEvent event) {
        notNull("event", event);
        serverDescriptionChangedEvents.add(event);
    }

    public ServerOpeningEvent getServerOpeningEvent() {
        return serverOpeningEvent;
    }

    public ServerClosedEvent getServerClosedEvent() {
        return serverClosedEvent;
    }

    public List<ServerDescriptionChangedEvent> getServerDescriptionChangedEvents() {
        return serverDescriptionChangedEvents;
    }
}
