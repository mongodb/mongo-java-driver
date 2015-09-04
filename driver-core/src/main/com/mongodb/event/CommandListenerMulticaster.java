/*
 * Copyright 2015 MongoDB, Inc.
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
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * A multicaster for connection events. Any Exception thrown by one of the listeners will be caught and not re-thrown, but may be
 * logged.
 *
 * @since 3.1
 *
 */
@Immutable
public class CommandListenerMulticaster implements CommandListener {
    private static final Logger LOGGER = Loggers.getLogger("protocol.event");

    private final List<CommandListener> commandListeners;

    /**
     * Construct an instance with the given list of command listeners
     *
     * @param commandListeners the non-null list of command listeners, none of which may be null
     */
    public CommandListenerMulticaster(final List<CommandListener> commandListeners) {
        notNull("commandListeners", commandListeners);
        for (CommandListener cur : commandListeners) {
            notNull("commandListener", cur);
        }
        this.commandListeners = new ArrayList<CommandListener>(commandListeners);
    }

    /**
     * Gets the command listeners.
     *
     * @return the unmodifiable set of command listeners
     */
    public List<CommandListener> getCommandListeners() {
        return Collections.unmodifiableList(commandListeners);
    }

    @Override
    public void commandStarted(final CommandStartedEvent event) {
        for (CommandListener cur : commandListeners) {
            try {
                cur.commandStarted(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising command started event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void commandSucceeded(final CommandSucceededEvent event) {
        for (CommandListener cur : commandListeners) {
            try {
                cur.commandSucceeded(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising command succeeded event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void commandFailed(final CommandFailedEvent event) {
        for (CommandListener cur : commandListeners) {
            try {
                cur.commandFailed(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising command failed event to listener %s", cur), e);
                }
            }
        }
    }
}
