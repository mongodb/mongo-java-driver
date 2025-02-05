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

import com.mongodb.lang.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

enum FaasEnvironment {
    AWS_LAMBDA("aws.lambda"),
    AZURE_FUNC("azure.func"),
    GCP_FUNC("gcp.func"),
    VERCEL("vercel"),
    UNKNOWN(null);

    static Map<String, String> envOverridesForTesting = new HashMap<>();

    static FaasEnvironment getFaasEnvironment() {
        List<FaasEnvironment> result = new ArrayList<>();
        String awsExecutionEnv = getEnv("AWS_EXECUTION_ENV");

        if (getEnv("VERCEL") != null) {
            result.add(FaasEnvironment.VERCEL);
        }
        if ((awsExecutionEnv != null && awsExecutionEnv.startsWith("AWS_Lambda_"))
            || getEnv("AWS_LAMBDA_RUNTIME_API") != null) {
            result.add(FaasEnvironment.AWS_LAMBDA);
        }
        if (getEnv("FUNCTIONS_WORKER_RUNTIME") != null) {
            result.add(FaasEnvironment.AZURE_FUNC);
        }
        if (getEnv("K_SERVICE") != null || getEnv("FUNCTION_NAME") != null) {
            result.add(FaasEnvironment.GCP_FUNC);
        }
        // vercel takes precedence over aws.lambda
        if (result.equals(Arrays.asList(FaasEnvironment.VERCEL, FaasEnvironment.AWS_LAMBDA))) {
            return FaasEnvironment.VERCEL;
        }
        if (result.size() != 1) {
            return FaasEnvironment.UNKNOWN;
        }
        return result.get(0);
    }

    @Nullable
    public static String getEnv(final String key) {
        if (envOverridesForTesting.containsKey(key)) {
            return envOverridesForTesting.get(key);
        }
        return System.getenv(key);
    }

    @Nullable
    private final String name;

    FaasEnvironment(@Nullable final String name) {
        this.name = name;
    }

    @Nullable
    public String getName() {
        return name;
    }

    @Nullable
    public Integer getTimeoutSec() {
        //noinspection SwitchStatementWithTooFewBranches
        switch (this) {
            case GCP_FUNC:
                return getEnvInteger("FUNCTION_TIMEOUT_SEC");
            default:
                return null;
        }
    }

    @Nullable
    public Integer getMemoryMb() {
        switch (this) {
            case AWS_LAMBDA:
                return getEnvInteger("AWS_LAMBDA_FUNCTION_MEMORY_SIZE");
            case GCP_FUNC:
                return getEnvInteger("FUNCTION_MEMORY_MB");
            default:
                return null;
        }
    }

    @Nullable
    public String getRegion() {
        switch (this) {
            case AWS_LAMBDA:
                return getEnv("AWS_REGION");
            case GCP_FUNC:
                return getEnv("FUNCTION_REGION");
            case VERCEL:
                return getEnv("VERCEL_REGION");
            default:
                return null;
        }
    }

    @Nullable
    private static Integer getEnvInteger(final String name) {
        try {
            String value = getEnv(name);
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
