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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.fail;
import static java.lang.String.format;
import static java.lang.System.getProperty;

public final class DriverInformationHelper {

    public static final DriverInformation INITIAL_DRIVER_INFORMATION =
            new DriverInformation(MongoDriverVersion.NAME, MongoDriverVersion.VERSION,
                    format("Java/%s/%s", getProperty("java.vendor", "unknown-vendor"),
                                         getProperty("java.runtime.version", "unknown-version")));

    private enum DriverField {
        DRIVER_NAME,
        DRIVER_VERSION,
        DRIVER_PLATFORM
    }

    public static List<String> getNames(final List<DriverInformation> driverInformation) {
        return getDriverInformation(DriverField.DRIVER_NAME, driverInformation);
    }

    public static List<String> getVersions(final List<DriverInformation> driverInformation) {
        return getDriverInformation(DriverField.DRIVER_VERSION, driverInformation);
    }

    public static List<String> getPlatforms(final List<DriverInformation> driverInformation) {
        return getDriverInformation(DriverField.DRIVER_PLATFORM, driverInformation);
    }

    private static List<String> getDriverInformation(final DriverField driverField, final List<DriverInformation> driverInformation) {
        List<String> data = new ArrayList<>();
        driverInformation.forEach(info -> {
            switch (driverField) {
                case DRIVER_NAME:
                    String name = info.getDriverName();
                    if (name != null) {
                        data.add(name);
                    }
                    break;
                case DRIVER_VERSION:
                    String version = info.getDriverVersion();
                    if (version != null) {
                        data.add(version);
                    }
                    break;
                case DRIVER_PLATFORM:
                    String platform = info.getDriverPlatform();
                    if (platform != null) {
                        data.add(platform);
                    }
                    break;
                default:
                    fail(format("Unknown field: %s", driverField));
            }
        });
        return Collections.unmodifiableList(data);
    }

    private DriverInformationHelper() {
    }
}
