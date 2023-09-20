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
package com.mongodb.internal;

import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;

import java.util.Objects;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Client Side Operation Timeout.
 *
 * <p>Includes support for the deprecated {@code maxTimeMS} and {@code maxCommitTimeMS} operation configurations</p>
 */
public class ClientSideOperationTimeout {

    private final TimeoutSettings timeoutSettings;

    @Nullable
    private final Timeout timeout;

    public ClientSideOperationTimeout(final TimeoutSettings timeoutSettings) {
        this(timeoutSettings, calculateTimeout(timeoutSettings.getTimeoutMS()));
    }

    ClientSideOperationTimeout(final TimeoutSettings timeoutSettings, @Nullable final Timeout timeout) {
        this.timeoutSettings = timeoutSettings;
        this.timeout = timeout;
    }

    /**
     * Allows for the differentiation between users explicitly setting a global operation timeout via {@code timeoutMS}.
     *
     * @return true if a timeout has been set.
     */
    public boolean hasTimeoutMS() {
        return timeoutSettings.getTimeoutMS() != null;
    }

    /**
     * Checks the expiry of the timeout.
     *
     * @return true if the timeout has been set and it has expired
     */
    public boolean expired() {
        return timeout != null && timeout.expired();
    }

    /**
     * Returns the remaining {@code timeoutMS} if set or the {@code alternativeTimeoutMS}.
     *
     * @param alternativeTimeoutMS the alternative timeout.
     * @return timeout to use.
     */
    public long timeoutOrAlternative(final long alternativeTimeoutMS) {
        Long timeoutMS = timeoutSettings.getTimeoutMS();
        if (timeoutMS == null) {
            return alternativeTimeoutMS;
        } else if (timeoutMS == 0) {
            return timeoutMS;
        } else {
            return timeoutRemainingMS();
        }
    }

    /**
     * Calculates the minimum timeout value between two possible timeouts.
     *
     * @param alternativeTimeoutMS the alternative timeout
     * @return the minimum value to use.
     */
    public long calculateMin(final long alternativeTimeoutMS) {
        Long timeoutMS = timeoutSettings.getTimeoutMS();
        if (timeoutMS == null) {
            return alternativeTimeoutMS;
        } else if (timeoutMS == 0) {
            return alternativeTimeoutMS;
        } else if (alternativeTimeoutMS == 0) {
            return timeoutRemainingMS();
        } else {
            return Math.min(timeoutRemainingMS(), alternativeTimeoutMS);
        }
    }

    public TimeoutSettings getTimeoutSettings() {
        return timeoutSettings;
    }

    public long getMaxAwaitTimeMS() {
        return timeoutSettings.getMaxAwaitTimeMS();
    }

    public long getMaxTimeMS() {
        return timeoutOrAlternative(timeoutSettings.getMaxTimeMS());
    }

    public long getMaxCommitTimeMS() {
        return timeoutOrAlternative(timeoutSettings.getMaxCommitTimeMS());
    }

    private long timeoutRemainingMS() {
        assertNotNull(timeout);
        return timeout.isInfinite() ? 0 : timeout.remaining(MILLISECONDS);
    }


    @Override
    public String toString() {
        return "ClientSideOperationTimeout{"
                + "timeoutContext=" + timeoutSettings
                + ", timeout=" + timeout
                + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClientSideOperationTimeout that = (ClientSideOperationTimeout) o;
        return Objects.equals(timeoutSettings, that.timeoutSettings) && Objects.equals(timeout, that.timeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeoutSettings, timeout);
    }

    @Nullable
    private static Timeout calculateTimeout(@Nullable final Long timeoutMS) {
        if (timeoutMS != null) {
            return timeoutMS == 0 ? Timeout.infinite() : Timeout.startNow(timeoutMS, MILLISECONDS);
        }
        return null;
    }
}
