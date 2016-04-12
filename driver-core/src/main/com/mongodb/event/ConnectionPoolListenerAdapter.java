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

/**
 * An adapter for connection pool listener implementations, for clients that want to listen for a subset of connection pool events. Extend
 * this class to listen for connection pool events and override the methods of interest.
 */
@Beta
public abstract class ConnectionPoolListenerAdapter implements ConnectionPoolListener {

    @Override
    public void connectionPoolOpened(final ConnectionPoolOpenedEvent event) {
    }

    @Override
    public void connectionPoolClosed(final ConnectionPoolClosedEvent event) {
    }

    @Override
    public void connectionCheckedOut(final ConnectionCheckedOutEvent event) {
    }

    @Override
    public void connectionCheckedIn(final ConnectionCheckedInEvent event) {
    }

    @Override
    public void waitQueueEntered(final ConnectionPoolWaitQueueEnteredEvent event) {
    }

    @Override
    public void waitQueueExited(final ConnectionPoolWaitQueueExitedEvent event) {
    }

    @Override
    public void connectionAdded(final ConnectionAddedEvent event) {
    }

    @Override
    public void connectionRemoved(final ConnectionRemovedEvent event) {
    }
}
