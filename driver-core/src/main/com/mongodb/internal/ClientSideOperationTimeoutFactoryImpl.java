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

import com.mongodb.lang.Nullable;

import java.util.Objects;

public final class ClientSideOperationTimeoutFactoryImpl implements ClientSideOperationTimeoutFactory {

    private final Long timeoutMS;
    private final long maxAwaitTimeMS;

    // Deprecated cursor based operation timeouts
    private final long maxTimeMS;
    private final long maxCommitMS;

    public ClientSideOperationTimeoutFactoryImpl(@Nullable final Long timeoutMS,
                                                 final long maxAwaitTimeMS,
                                                 final long maxTimeMS,
                                                 final long maxCommitMS) {
        this.timeoutMS = timeoutMS;
        this.maxAwaitTimeMS = maxAwaitTimeMS;
        this.maxTimeMS = timeoutMS == null ? maxTimeMS : 0;
        this.maxCommitMS = timeoutMS == null ? maxCommitMS : 0;
    }

    public ClientSideOperationTimeout create() {
        return new ClientSideOperationTimeout(timeoutMS, maxAwaitTimeMS, maxTimeMS, maxCommitMS);
    }

    @Override
    public String toString() {
        return "ClientSideOperationTimeoutFactoryImpl{"
                + "timeoutMS=" + timeoutMS
                + ", maxAwaitTimeMS=" + maxAwaitTimeMS
                + ", maxTimeMS=" + maxTimeMS
                + ", maxCommitMS=" + maxCommitMS
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
        final ClientSideOperationTimeoutFactoryImpl that = (ClientSideOperationTimeoutFactoryImpl) o;
        return maxTimeMS == that.maxTimeMS && maxCommitMS == that.maxCommitMS
                && Objects.equals(timeoutMS, that.timeoutMS)
                && Objects.equals(maxAwaitTimeMS, that.maxAwaitTimeMS);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeoutMS, maxAwaitTimeMS, maxTimeMS, maxCommitMS);
    }
}
