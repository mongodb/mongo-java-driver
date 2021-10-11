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

package com.mongodb.client;

import com.mongodb.ConnectionString;
import org.bson.Document;
import org.junit.jupiter.params.provider.Arguments;

import java.util.Arrays;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.getConnectionString;

public final class ConnectivityTestHelper {
    public static final Document LEGACY_HELLO_COMMAND = new Document("ismaster", 1);

    /**
     * Gets the Junit Arguments for connectivity tests that use the "|"-delimited system property "org.mongodb.test.connectivity.uris"
     * for the list of connection strings for which to check connectivity. If that system property is not set, it uses the default
     * connection string configured for the entire test run (which itself defaults to "mongodb://localhost).
     *
     * @return a {@code Stream<Arguments>}: the first argument is of type {@code ConnectionString}, the second is of type
     * {@code List<String>}, representing the list of hosts on the connection string.  The latter is useful as the value of the name of
     * the parameterized test, e.g. {@code @ParameterizedTest(name = "{1}")}.
     */
    public static Stream<Arguments> getConnectivityTestArguments() {
        String connectionStrings = System.getProperty("org.mongodb.test.connectivity.uris",
                getConnectionString().getConnectionString());
        return Arrays.stream(connectionStrings.split("\\|"))
                .map(str -> {
                    ConnectionString connectionString = new ConnectionString(str);
                    return Arguments.of(connectionString, connectionString.getHosts());
                });
    }

    private ConnectivityTestHelper() {
    }
}
