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

package com.mongodb.internal.connection;

import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

class TestClusterListener implements ClusterListener {
    private ClusterOpeningEvent clusterOpeningEvent;
    private ClusterClosedEvent clusterClosingEvent;
    private final List<ClusterDescriptionChangedEvent> clusterDescriptionChangedEvents = new ArrayList<>();

    @Override
    public void clusterOpening(final ClusterOpeningEvent event) {
        isTrue("clusterOpeningEvent is null", clusterOpeningEvent == null);
        clusterOpeningEvent = event;
    }

    @Override
    public void clusterClosed(final ClusterClosedEvent event) {
        isTrue("clusterClosingEvent is null", clusterClosingEvent == null);
        clusterClosingEvent = event;
    }

    @Override
    public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
        notNull("event", event);
        clusterDescriptionChangedEvents.add(event);
    }

    public ClusterOpeningEvent getClusterOpeningEvent() {
        return clusterOpeningEvent;
    }

    public ClusterClosedEvent getClusterClosingEvent() {
        return clusterClosingEvent;
    }

    public List<ClusterDescriptionChangedEvent> getClusterDescriptionChangedEvents() {
        return clusterDescriptionChangedEvents;
    }

    public void clearClusterDescriptionChangedEvents() {
        clusterDescriptionChangedEvents.clear();
    }
}
