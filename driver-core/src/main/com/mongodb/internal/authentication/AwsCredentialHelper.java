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
        try {
            Class.forName("software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider");
            awsCredentialSupplier = new AwsSdkV2CredentialSupplier();
            LOGGER.info("Using software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider from AWS SDK v2 to retrieve AWS "
                    + "credentials");
        } catch (ClassNotFoundException e) {
            awsCredentialSupplier = new BuiltInAwsCredentialSupplier();
            LOGGER.info("Using built-in driver implementation to retrieve AWS credentials. Consider adding a dependency to "
                    + "software.amazon.awssdk:auth to get access to additional AWS authentication functionality");
        }
    }

    /**
     * This method is visible to allow tests to require the built-in provider rather than rely on the fixed checks for classes on the
     * classpath.  It allows us to easily write tests of both implementations without resorting to runtime classpath shenanigans.
     */
    @VisibleForTesting(otherwise = PRIVATE)
    public static void requireBuiltInProvider() {
        LOGGER.info("Using built-in driver implementation to retrieve AWS credentials");
        awsCredentialSupplier = new BuiltInAwsCredentialSupplier();
    }

    public static AwsCredential obtainFromEnvironment() {
        return awsCredentialSupplier.get();
    }

    private AwsCredentialHelper() {
    }
}
