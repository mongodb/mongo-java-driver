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

import com.mongodb.annotations.Immutable;

import java.util.List;

/**
 * A multicaster for connection events. Any Exception thrown by one of the listeners will be caught and not re-thrown, but may be
 * logged.
 *
 * @since 3.1
 * @deprecated register multiple command listeners in the settings
 */
@Immutable
@Deprecated
@SuppressWarnings("deprecation")
public class CommandListenerMulticaster implements CommandListener {

    private final CommandEventMulticaster wrapped;

    /**
     * Construct an instance with the given list of command listeners
     *
     * @param commandListeners the non-null list of command listeners, none of which may be null
     */
    public CommandListenerMulticaster(final List<CommandListener> commandListeners) {
        wrapped = new CommandEventMulticaster(commandListeners);
    }

    /**
     * Gets the command listeners.
     *
     * @return the unmodifiable set of command listeners
     */
    public List<CommandListener> getCommandListeners() {
        return wrapped.getCommandListeners();
    }

    @Override
    public void commandStarted(final CommandStartedEvent event) {
        wrapped.commandStarted(event);
    }

    @Override
    public void commandSucceeded(final CommandSucceededEvent event) {
       wrapped.commandSucceeded(event);
    }

    @Override
    public void commandFailed(final CommandFailedEvent event) {
        wrapped.commandFailed(event);
    }
}
