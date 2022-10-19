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

package com.mongodb.internal.authentication;

import com.mongodb.AwsCredential;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.VisibleForTesting;

import java.util.function.Supplier;

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * Utility class for working with AWS authentication.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public final class AwsCredentialHelper {
    public static final Logger LOGGER = Loggers.getLogger("authenticator");

    private static volatile Supplier<AwsCredential> awsCredentialSupplier;

    static {
        if (isClassAvailable("software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")) {
            awsCredentialSupplier = new AwsSdkV2CredentialSupplier();
            LOGGER.info("Using DefaultCredentialsProvider from AWS SDK v2 to retrieve AWS credentials. This is the recommended "
                    + "configuration");
        } else if (isClassAvailable("com.amazonaws.auth.DefaultAWSCredentialsProviderChain")) {
            awsCredentialSupplier = new AwsSdkV1CredentialSupplier();
            LOGGER.info("Using DefaultAWSCredentialsProviderChain from AWS SDK v1 to retrieve AWS credentials. Consider adding a "
                    + "dependency to AWS SDK v2's software.amazon.awssdk:auth artifact to get access to additional AWS authentication "
                    + "functionality.");
        } else {
            awsCredentialSupplier = new BuiltInAwsCredentialSupplier();
            LOGGER.info("Using built-in driver implementation to retrieve AWS credentials. Consider adding a dependency to AWS SDK "
                    + "v2's software.amazon.awssdk:auth artifact to get access to additional AWS authentication functionality.");
        }
    }

    private static boolean isClassAvailable(final String className) {
        try {
            Class.forName(className);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * This method is visible to allow tests to require the built-in provider rather than rely on the fixed checks for classes on the
     * classpath.  It allows us to easily write tests of the built-in implementation without resorting to runtime classpath shenanigans.
     */
    @VisibleForTesting(otherwise = PRIVATE)
    public static void requireBuiltInProvider() {
        LOGGER.info("Using built-in driver implementation to retrieve AWS credentials");
        awsCredentialSupplier = new BuiltInAwsCredentialSupplier();
    }

    /**
     * This method is visible to allow tests to require the AWS SDK v1 provider rather than rely on the fixed checks for classes on the
     * classpath.  It allows us to easily write tests of the AWS SDK v1 implementation without resorting to runtime classpath shenanigans.
     */
    @VisibleForTesting(otherwise = PRIVATE)
    public static void requireAwsSdkV1Provider() {
        LOGGER.info("Using AWS SDK v1 to retrieve AWS credentials");
        awsCredentialSupplier = new AwsSdkV1CredentialSupplier();
    }

    /**
     * This method is visible to allow tests to require the AWS SDK v2 provider rather than rely on the fixed checks for classes on the
     * classpath.  It allows us to easily write tests of the AWS SDK v2 implementation without resorting to runtime classpath shenanigans.
     */
    @VisibleForTesting(otherwise = PRIVATE)
    public static void requireAwsSdkV2Provider() {
        LOGGER.info("Using AWS SDK v2 to retrieve AWS credentials");
        awsCredentialSupplier = new AwsSdkV2CredentialSupplier();
    }

    public static AwsCredential obtainFromEnvironment() {
        return awsCredentialSupplier.get();
    }

    private AwsCredentialHelper() {
    }
}
