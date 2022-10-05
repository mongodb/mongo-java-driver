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

/**
 * This class is NOT part of the public API. It may change at any time without notification.
 */
public final class ServerVersionHelper {

    public static final int MIN_WIRE_VERSION = 0;
    public static final int THREE_DOT_SIX_WIRE_VERSION = 6;
    public static final int FOUR_DOT_ZERO_WIRE_VERSION = 7;
    public static final int FOUR_DOT_TWO_WIRE_VERSION = 8;
    public static final int FOUR_DOT_FOUR_WIRE_VERSION = 9;
    public static final int FIVE_DOT_ZERO_WIRE_VERSION = 12;
    public static final int SIX_DOT_ZERO_WIRE_VERSION = 17;

    public static boolean serverIsAtLeastVersionFourDotZero(final ConnectionDescription description) {
        return description.getMaxWireVersion() >= FOUR_DOT_ZERO_WIRE_VERSION;
    }

    public static boolean serverIsAtLeastVersionFourDotTwo(final ConnectionDescription description) {
        return description.getMaxWireVersion() >= FOUR_DOT_TWO_WIRE_VERSION;
    }

    public static boolean serverIsAtLeastVersionFourDotFour(final ConnectionDescription description) {
        return description.getMaxWireVersion() >= FOUR_DOT_FOUR_WIRE_VERSION;
    }

    public static boolean serverIsAtLeastVersionFiveDotZero(final ConnectionDescription description) {
        return description.getMaxWireVersion() >= FIVE_DOT_ZERO_WIRE_VERSION;
    }

    public static boolean serverIsLessThanVersionFourDotZero(final ConnectionDescription description) {
        return description.getMaxWireVersion() < FOUR_DOT_ZERO_WIRE_VERSION;
    }

    public static boolean serverIsLessThanVersionFourDotTwo(final ConnectionDescription description) {
        return description.getMaxWireVersion() < FOUR_DOT_TWO_WIRE_VERSION;
    }

    public static boolean serverIsLessThanVersionFourDotFour(final ConnectionDescription description) {
        return description.getMaxWireVersion() < FOUR_DOT_FOUR_WIRE_VERSION;
    }

    private ServerVersionHelper() {
    }
}
