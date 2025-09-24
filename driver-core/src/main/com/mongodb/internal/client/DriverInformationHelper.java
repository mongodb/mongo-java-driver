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

import com.mongodb.internal.build.MongoDriverVersion;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.lang.System.getProperty;

public final class DriverInformationHelper {

    public static final DriverInformation INITIAL_DRIVER_INFORMATION =
            new DriverInformation(MongoDriverVersion.NAME, MongoDriverVersion.VERSION,
                    format("Java/%s/%s", getProperty("java.vendor", "unknown-vendor"),
                                         getProperty("java.runtime.version", "unknown-version")));

    public static List<String> getNames(final List<DriverInformation> driverInformation) {
        return getDriverField(DriverInformation::getDriverName, driverInformation);
    }

    public static List<String> getVersions(final List<DriverInformation> driverInformation) {
        return getDriverField(DriverInformation::getDriverVersion, driverInformation);
    }

    public static List<String> getPlatforms(final List<DriverInformation> driverInformation) {
        return getDriverField(DriverInformation::getDriverPlatform, driverInformation);
    }

    private static List<String> getDriverField(final Function<DriverInformation, String> fieldSupplier,
                                                                          final List<DriverInformation> driverInformation) {
        return Collections.unmodifiableList(driverInformation.stream()
                .map(fieldSupplier)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
    }

    private DriverInformationHelper() {
    }
}
