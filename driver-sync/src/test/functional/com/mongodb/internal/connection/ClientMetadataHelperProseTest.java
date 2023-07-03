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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoDriverInformation;
import com.mongodb.MongoNamespace;
import com.mongodb.client.TestHelper;
import com.mongodb.client.test.CollectionHelper;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.TestHelper.repeat;
import static com.mongodb.client.TestHelper.withSystemProperty;
import static com.mongodb.internal.connection.ClientMetadataHelperSpecification.createExpectedClientMetadataDocument;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * See <a href="https://github.com/mongodb/specifications/blob/master/source/mongodb-handshake/handshake.rst#test-plan">spec</a>
 * @see ClientMetadataHelperSpecification
 */
public class ClientMetadataHelperProseTest {
    private static final String APP_NAME = "app name";

    @AfterEach
    public void afterEach() {
        TestHelper.resetEnvironmentVariables();
        ClientMetadataHelper.resetCachedMetadataDocument();
    }

    @Test
    public void test01ValidAws() {
        TestHelper.setEnvironmentVariable("AWS_EXECUTION_ENV", "AWS_Lambda_java8");
        TestHelper.setEnvironmentVariable("AWS_REGION", "us-east-2");
        TestHelper.setEnvironmentVariable("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "1024");

        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
        expected.put("env", BsonDocument.parse("{'name': 'aws.lambda', 'memory_mb': 1024, 'region': 'us-east-2'}"));
        BsonDocument actual = getClientMetadataDocument();
        assertEquals(expected, actual);

        performHello();
    }

    @Test
    public void test02ValidAzure() {
        TestHelper.setEnvironmentVariable("FUNCTIONS_WORKER_RUNTIME", "node");

        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
        expected.put("env", BsonDocument.parse("{'name': 'azure.func'}"));
        BsonDocument actual = getClientMetadataDocument();
        assertEquals(expected, actual);

        performHello();
    }

    @Test
    public void test03ValidGcp() {
        TestHelper.setEnvironmentVariable("K_SERVICE", "servicename");
        TestHelper.setEnvironmentVariable("FUNCTION_MEMORY_MB", "1024");
        TestHelper.setEnvironmentVariable("FUNCTION_TIMEOUT_SEC", "60");
        TestHelper.setEnvironmentVariable("FUNCTION_REGION", "us-central1");

        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
        expected.put("env", BsonDocument.parse(
                "{'name': 'gcp.func', 'timeout_sec': 60, 'memory_mb': 1024, 'region': 'us-central1'}"));
        BsonDocument actual = getClientMetadataDocument();
        assertEquals(expected, actual);

        performHello();
    }

    @Test
    public void test04ValidVercel() {
        TestHelper.setEnvironmentVariable("VERCEL", "1");
        TestHelper.setEnvironmentVariable("VERCEL_URL", "*.vercel.app");
        TestHelper.setEnvironmentVariable("VERCEL_REGION", "cdg1");

        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
        expected.put("env", BsonDocument.parse("{'name': 'vercel', 'url': '*.vercel.app', 'region': 'cdg1'}"));
        BsonDocument actual = getClientMetadataDocument();
        assertEquals(expected, actual);

        performHello();
    }

    @Test
    public void test05InvalidMultipleProviders() {
        TestHelper.setEnvironmentVariable("AWS_EXECUTION_ENV", "AWS_Lambda_java8");
        TestHelper.setEnvironmentVariable("FUNCTIONS_WORKER_RUNTIME", "node");

        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
        BsonDocument actual = getClientMetadataDocument();
        assertEquals(expected, actual);

        performHello();
    }

    @Test
    public void test06InvalidLongString() {
        TestHelper.setEnvironmentVariable("AWS_EXECUTION_ENV", "AWS_Lambda_java8");
        TestHelper.setEnvironmentVariable("AWS_REGION", repeat(512, "a"));

        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
        expected.put("env", BsonDocument.parse("{'name': 'aws.lambda'}"));
        BsonDocument actual = getClientMetadataDocument();
        assertEquals(expected, actual);

        performHello();
    }

    @Test
    public void test07InvalidWrongTypes() {
        TestHelper.setEnvironmentVariable("AWS_EXECUTION_ENV", "AWS_Lambda_java8");
        TestHelper.setEnvironmentVariable("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "big");

        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
        expected.put("env", BsonDocument.parse("{'name': 'aws.lambda'}"));
        BsonDocument actual = getClientMetadataDocument();
        assertEquals(expected, actual);

        performHello();
    }


    @Test
    public void test08NotLambda() {
        TestHelper.setEnvironmentVariable("AWS_EXECUTION_ENV", "EC2");

        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
        BsonDocument actual = getClientMetadataDocument();
        assertEquals(expected, actual);

        performHello();
    }

    // Additional tests, not specified as prose tests:

    @Test
    public void testLimitForDriverVersion() {
        // should create client metadata document and exclude the extra driver info if its too verbose
        MongoDriverInformation driverInfo = MongoDriverInformation.builder()
                .driverName("mongo-spark")
                .driverVersion(repeat(512, "a"))
                .driverPlatform("Scala 2.10 / Spark 2.0.0")
                .build();

        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME, driverInfo);
        BsonDocument expectedBase = createExpectedClientMetadataDocument(APP_NAME);
        expected.put("driver", expectedBase.get("driver"));

        BsonDocument actual = ClientMetadataHelper.getClientMetadataDocument(APP_NAME, driverInfo);
        assertEquals(expected, actual);
    }

    @Test
    public void testLimitForPlatform() {
        MongoDriverInformation driverInfo = MongoDriverInformation.builder()
                .driverName("mongo-spark")
                .driverVersion("2.0.0")
                .driverPlatform(repeat(512, "a"))
                .build();

        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME, driverInfo);
        BsonDocument expectedBase = createExpectedClientMetadataDocument(APP_NAME);
        expected.put("platform", expectedBase.get("platform"));

        BsonDocument actual = ClientMetadataHelper.getClientMetadataDocument(APP_NAME, driverInfo);
        assertEquals(expected, actual);
    }

    @Test
    public void testLimitForOsName() {
        withSystemProperty("os.name", repeat(512, "a"), () -> {
            BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
            expected.getDocument("os").remove("name");

            BsonDocument actual = getClientMetadataDocument();
            assertEquals(expected, actual);
        });
    }

    private void performHello() {
        CollectionHelper<Document> collectionHelper = new CollectionHelper<>(
                new DocumentCodec(),
                new MongoNamespace(ClusterFixture.getDefaultDatabaseName(), "test"));
        collectionHelper.hello();
    }

    private BsonDocument getClientMetadataDocument() {
        return ClientMetadataHelper.getClientMetadataDocument(APP_NAME, null);
    }
}
