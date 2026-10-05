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
package com.mongodb.internal.client;

import com.mongodb.lang.Nullable;

import java.util.Objects;

public final class DriverInformation {
    @Nullable
    private final String driverName;
    @Nullable
    private final String driverVersion;
    @Nullable
    private final String driverPlatform;

    public DriverInformation(@Nullable final String driverName,
            @Nullable final String driverVersion,
            @Nullable final String driverPlatform) {
        this.driverName = driverName == null || driverName.isEmpty() ? null : driverName;
        this.driverVersion = driverVersion == null || driverVersion.isEmpty() ? null : driverVersion;
        this.driverPlatform = driverPlatform == null || driverPlatform.isEmpty() ? null : driverPlatform;
    }

    @Nullable
    public String getDriverName() {
        return driverName;
    }

    @Nullable
    public String getDriverVersion() {
        return driverVersion;
    }

    @Nullable
    public String getDriverPlatform() {
        return driverPlatform;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final DriverInformation that = (DriverInformation) o;
        return Objects.equals(driverName, that.driverName)
                && Objects.equals(driverVersion, that.driverVersion)
                && Objects.equals(driverPlatform, that.driverPlatform);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(driverName);
        result = 31 * result + Objects.hashCode(driverVersion);
        result = 31 * result + Objects.hashCode(driverPlatform);
        return result;
    }

    @Override
    public String toString() {
        return "DriverInformation{"
                + "driverName='" + driverName + '\''
                + ", driverVersion='" + driverVersion + '\''
                + ", driverPlatform='" + driverPlatform + '\''
                + '}';
    }
}
