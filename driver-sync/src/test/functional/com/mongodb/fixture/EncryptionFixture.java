/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package com.mongodb.fixture;

import java.util.HashMap;
import java.util.Map;

import static com.mongodb.ClusterFixture.getEnv;

/**
 * Helper class for the CSFLE/QE tests.
 */
public final class EncryptionFixture {

    private EncryptionFixture() {
        //NOP
    }

    public static Map<String, Map<String, Object>> getKmsProviders(final KmsProviderType... kmsProviderTypes) {
        return new HashMap<String, Map<String, Object>>() {{
            for (KmsProviderType kmsProviderType : kmsProviderTypes) {
                switch (kmsProviderType) {
                    case LOCAL:
                        put("local", new HashMap<String, Object>() {{
                            put("key", "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBM"
                                    + "UN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk");
                        }});
                        break;
                    case GCP:
                        put("gcp", new HashMap<String, Object>() {{
                            put("email", getEnv("GCP_EMAIL"));
                            put("privateKey", getEnv("GCP_PRIVATE_KEY"));
                        }});
                        break;
                    case AWS:
                        put("aws", new HashMap<String, Object>() {{
                            put("accessKeyId", getEnv("AWS_ACCESS_KEY_ID"));
                            put("secretAccessKey", getEnv("AWS_SECRET_ACCESS_KEY"));
                        }});
                        break;
                    case AZURE:
                        put("azure", new HashMap<String, Object>() {{
                            put("tenantId", getEnv("AZURE_TENANT_ID"));
                            put("clientId", getEnv("AZURE_CLIENT_ID"));
                            put("clientSecret", getEnv("AZURE_CLIENT_SECRET"));
                        }});
                        break;
                    case KMIP:
                        put("kmip", new HashMap<String, Object>() {{
                            put("endpoint", getEnv("org.mongodb.test.kmipEndpoint", "localhost:5698"));
                        }});
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported KMS provider type: " + kmsProviderType);
                }
            }
        }};
    }

    public enum KmsProviderType {
        LOCAL,
        AWS,
        AZURE,
        GCP,
        KMIP
    }
}
