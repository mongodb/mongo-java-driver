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

import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionClosedEvent;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class EventReasonMessageResolver {
    private static final String MESSAGE_CONNECTION_POOL_WAS_CLOSED = "Connection pool was closed";
    private static final String EMPTY_REASON = "";

    public static String getMessage(final ConnectionClosedEvent.Reason reason) {
        switch (reason) {
            case STALE:
                return "Connection became stale because the pool was cleared";
            case IDLE:
                return "Connection has been available but unused for longer than the configured max idle time";
            case ERROR:
                return "An error occurred while using the connection";
            case POOL_CLOSED:
                return MESSAGE_CONNECTION_POOL_WAS_CLOSED;
            default:
                return EMPTY_REASON;
        }
    }

    public static String getMessage(final ConnectionCheckOutFailedEvent.Reason reason) {
        switch (reason) {
            case TIMEOUT:
                return "Wait queue timeout elapsed without a connection becoming available";
            case CONNECTION_ERROR:
                return "An error occurred while trying to establish a new connection";
            case POOL_CLOSED:
                return MESSAGE_CONNECTION_POOL_WAS_CLOSED;
            default:
                return "";
        }
    }

    private EventReasonMessageResolver() {
        //NOP
    }
}
