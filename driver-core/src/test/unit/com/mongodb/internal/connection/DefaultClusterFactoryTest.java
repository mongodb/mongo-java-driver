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
package com.mongodb.internal.connection;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.mongodb.ConnectionString;
import com.mongodb.connection.ClusterSettings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class DefaultClusterFactoryTest {
    private static final String EXPECTED_COSMOS_DB_MESSAGE =
            "You appear to be connected to a CosmosDB cluster. For more information regarding "
                    + "feature compatibility and support please visit https://www.mongodb.com/supportability/cosmosdb";

    private static final String EXPECTED_DOCUMENT_DB_MESSAGE =
            "You appear to be connected to a DocumentDB cluster. For more information regarding "
                    + "feature compatibility and support please visit https://www.mongodb.com/supportability/documentdb";

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger("org.mongodb.driver.client");
    private static final MemoryAppender MEMORY_APPENDER = new MemoryAppender();

    @BeforeAll
    public static void setUp() {
        MEMORY_APPENDER.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        LOGGER.setLevel(Level.DEBUG);
        LOGGER.addAppender(MEMORY_APPENDER);
        MEMORY_APPENDER.start();
    }

    @AfterAll
    public static void cleanUp() {
        LOGGER.detachAppender(MEMORY_APPENDER);
    }

    @AfterEach
    public void reset() {
        MEMORY_APPENDER.reset();
    }

    static Stream<Arguments> shouldLogAllegedClusterEnvironmentWhenNonGenuineHostsSpecified() {
        return Stream.of(
                Arguments.of("mongodb://a.MONGO.COSMOS.AZURE.COM:19555", EXPECTED_COSMOS_DB_MESSAGE),
                Arguments.of("mongodb://a.mongo.cosmos.azure.com:19555", EXPECTED_COSMOS_DB_MESSAGE),
                Arguments.of("mongodb://a.DOCDB-ELASTIC.AMAZONAWS.COM:27017/", EXPECTED_DOCUMENT_DB_MESSAGE),
                Arguments.of("mongodb://a.docdb-elastic.amazonaws.com:27017/", EXPECTED_DOCUMENT_DB_MESSAGE),
                Arguments.of("mongodb://a.DOCDB.AMAZONAWS.COM", EXPECTED_DOCUMENT_DB_MESSAGE),
                Arguments.of("mongodb://a.docdb.amazonaws.com", EXPECTED_DOCUMENT_DB_MESSAGE),

                /* SRV matching */
                Arguments.of("mongodb+srv://A.MONGO.COSMOS.AZURE.COM", EXPECTED_COSMOS_DB_MESSAGE),
                Arguments.of("mongodb+srv://a.mongo.cosmos.azure.com", EXPECTED_COSMOS_DB_MESSAGE),
                Arguments.of("mongodb+srv://a.DOCDB.AMAZONAWS.COM/", EXPECTED_DOCUMENT_DB_MESSAGE),
                Arguments.of("mongodb+srv://a.docdb.amazonaws.com/", EXPECTED_DOCUMENT_DB_MESSAGE),
                Arguments.of("mongodb+srv://a.DOCDB-ELASTIC.AMAZONAWS.COM/", EXPECTED_DOCUMENT_DB_MESSAGE),
                Arguments.of("mongodb+srv://a.docdb-elastic.amazonaws.com/", EXPECTED_DOCUMENT_DB_MESSAGE),

                /* Mixing genuine and nongenuine hosts (unlikely in practice) */
                Arguments.of("mongodb://a.example.com:27017,b.mongo.cosmos.azure.com:19555/", EXPECTED_COSMOS_DB_MESSAGE),
                Arguments.of("mongodb://a.example.com:27017,b.docdb.amazonaws.com:27017/", EXPECTED_DOCUMENT_DB_MESSAGE),
                Arguments.of("mongodb://a.example.com:27017,b.docdb-elastic.amazonaws.com:27017/", EXPECTED_DOCUMENT_DB_MESSAGE)
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldLogAllegedClusterEnvironmentWhenNonGenuineHostsSpecified(final String connectionString, final String expectedLogMessage) {
        //when
        ClusterSettings clusterSettings = toClusterSettings(new ConnectionString(connectionString));
        new DefaultClusterFactory().detectAndLogClusterEnvironment(clusterSettings);

        //then
        List<ILoggingEvent> loggedEvents = MEMORY_APPENDER.search(expectedLogMessage);

        Assertions.assertEquals(1, loggedEvents.size());
        Assertions.assertEquals(Level.INFO, loggedEvents.get(0).getLevel());

    }

    static Stream<String> shouldNotLogClusterEnvironmentWhenGenuineHostsSpecified() {
        return Stream.of(
                "mongodb://a.mongo.cosmos.azure.com.tld:19555",
                "mongodb://a.docdb-elastic.amazonaws.com.t",
                "mongodb+srv://a.example.com",
                "mongodb+srv://a.mongodb.net/",
                "mongodb+srv://a.mongo.cosmos.azure.com.tld/",
                "mongodb+srv://a.docdb-elastic.amazonaws.com.tld/"
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldNotLogClusterEnvironmentWhenGenuineHostsSpecified(final String connectionUrl) {
        //when
        ClusterSettings clusterSettings = toClusterSettings(new ConnectionString(connectionUrl));
        new DefaultClusterFactory().detectAndLogClusterEnvironment(clusterSettings);

        //then
        Assertions.assertEquals(0, MEMORY_APPENDER.search(EXPECTED_COSMOS_DB_MESSAGE).size());
        Assertions.assertEquals(0, MEMORY_APPENDER.search(EXPECTED_DOCUMENT_DB_MESSAGE).size());
    }

    private static ClusterSettings toClusterSettings(final ConnectionString connectionUrl) {
        return ClusterSettings.builder().applyConnectionString(connectionUrl).build();
    }

    public static class MemoryAppender extends ListAppender<ILoggingEvent> {
        public void reset() {
            this.list.clear();
        }

        public List<ILoggingEvent> search(final String message) {
            return this.list.stream()
                    .filter(event -> event.getFormattedMessage().contains(message))
                    .collect(Collectors.toList());
        }
    }
}
