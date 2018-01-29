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

/**
 * A listener for command events
 *
 * @since 3.1
 */
public interface CommandListener {
    /**
     * Listener for command started events.
     *
     * @param event the event
     */
    void commandStarted(CommandStartedEvent event);

    /**
     * Listener for command completed events
     *
     * @param event the event
     */
    void commandSucceeded(CommandSucceededEvent event);

    /**
     * Listener for command failure events
     *
     * @param event the event
     */
    void commandFailed(CommandFailedEvent event);
}
