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

package com.mongodb.internal.event;

import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static java.lang.String.format;


final class CommandListenerMulticaster implements CommandListener {
    private static final Logger LOGGER = Loggers.getLogger("protocol.event");

    private final List<CommandListener> commandListeners;

    CommandListenerMulticaster(final List<CommandListener> commandListeners) {
        isTrue("All CommandListener instances are non-null", !commandListeners.contains(null));
        this.commandListeners = new ArrayList<CommandListener>(commandListeners);
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
