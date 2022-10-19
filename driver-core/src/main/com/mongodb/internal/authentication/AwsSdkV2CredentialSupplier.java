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
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.util.function.Supplier;

public final class AwsSdkV2CredentialSupplier implements Supplier<AwsCredential> {

    private final AwsCredentialsProvider provider = DefaultCredentialsProvider.create();

    @Override
    public AwsCredential get() {
        AwsCredentials credentials = provider.resolveCredentials();
        if (credentials instanceof AwsSessionCredentials) {
            AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) credentials;
            return new AwsCredential(sessionCredentials.accessKeyId(), sessionCredentials.secretAccessKey(),
                    sessionCredentials.sessionToken());
        } else {
            return new AwsCredential(credentials.accessKeyId(), credentials.secretAccessKey(), null);
        }
    }
}
