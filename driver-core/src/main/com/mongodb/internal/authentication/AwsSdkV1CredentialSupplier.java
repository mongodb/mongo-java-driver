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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.mongodb.AwsCredential;

import java.util.function.Supplier;

public final class AwsSdkV1CredentialSupplier implements Supplier<AwsCredential> {

    private final AWSCredentialsProvider provider = DefaultAWSCredentialsProviderChain.getInstance();

    @Override
    public AwsCredential get() {
        AWSCredentials credentials = provider.getCredentials();
        if (credentials instanceof AWSSessionCredentials) {
            AWSSessionCredentials sessionCredentials = (AWSSessionCredentials) credentials;
            return new AwsCredential(sessionCredentials.getAWSAccessKeyId(), sessionCredentials.getAWSSecretKey(),
                    sessionCredentials.getSessionToken());
        } else {
            return new AwsCredential(credentials.getAWSAccessKeyId(), credentials.getAWSSecretKey(), null);
        }
    }
}
