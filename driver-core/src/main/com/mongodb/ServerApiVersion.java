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

package com.mongodb;

/**
 * The Server API version.
 *
 * @since 4.3
 */
public enum ServerApiVersion {
    /**
     * Server API version 1
     */
    V1("1");

    private final String versionString;

    ServerApiVersion(final String versionString) {
        this.versionString = versionString;
    }

    /**
     * Gets the version as a string.
     *
     * @return the version string
     */
    public String getValue() {
        return versionString;
    }

    /**
     * Gets the {@code ServerApiVersion} that corresponds to the given value.
     *
     * @param value the String value of the desired server API version
     * @return the corresponding {@code ServerApiVersion}
     * @throws MongoClientException if no matching enumeration exists
     */
    public static ServerApiVersion findByValue(final String value) {
        //noinspection SwitchStatementWithTooFewBranches
        switch (value) {
            case "1":
                return V1;
            default:
                throw new MongoClientException("Unsupported server API version: " + value);

        }
    }
}
