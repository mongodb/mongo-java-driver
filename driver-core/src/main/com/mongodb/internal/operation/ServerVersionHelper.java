/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.operation;


import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerVersion;

/**
 * This class is NOT part of the public API. It may change at any time without notification.
 */
public final class ServerVersionHelper {

    public static boolean serverIsAtLeastVersionThreeDotZero(final ConnectionDescription description) {
        return serverIsAtLeastVersion(description, new ServerVersion(3, 0));
    }

    public static boolean serverIsAtLeastVersionThreeDotTwo(final ConnectionDescription description) {
        return serverIsAtLeastVersion(description, new ServerVersion(3, 2));
    }

    public static boolean serverIsAtLeastVersionThreeDotFour(final ConnectionDescription description) {
        return serverIsAtLeastVersion(description, new ServerVersion(3, 4));
    }

    public static boolean serverIsAtLeastVersionThreeDotSix(final ConnectionDescription description) {
        return serverIsAtLeastVersion(description, new ServerVersion(3, 6));
    }

    public static boolean serverIsAtLeastVersionFourDotZero(final ConnectionDescription description) {
        return serverIsAtLeastVersion(description, new ServerVersion(4, 0));
    }

    public static boolean serverIsAtLeastVersionFourDotFour(final ConnectionDescription description) {
        return serverIsAtLeastVersion(description, new ServerVersion(4, 4));
    }

    private static boolean serverIsAtLeastVersion(final ConnectionDescription description, final ServerVersion serverVersion) {
        return description.getServerVersion().compareTo(serverVersion) >= 0;
    }

    private ServerVersionHelper() {
    }
}
