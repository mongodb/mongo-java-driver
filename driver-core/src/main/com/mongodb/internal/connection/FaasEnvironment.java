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
import java.util.List;

enum FaasEnvironment {
    AWS_LAMBDA("aws.lambda"),
    AZURE_FUNC("azure.func"),
    GCP_FUNC("gcp.func"),
    VERCEL("vercel"),
    UNKNOWN(null);

    static FaasEnvironment getFaasEnvironment() {
        List<FaasEnvironment> result = new ArrayList<>();
        String awsExecutionEnv = System.getenv("AWS_EXECUTION_ENV");

        if (System.getenv("VERCEL") != null) {
            result.add(FaasEnvironment.VERCEL);
        }
        if ((awsExecutionEnv != null && awsExecutionEnv.startsWith("AWS_Lambda_"))
                || System.getenv("AWS_LAMBDA_RUNTIME_API") != null) {
            result.add(FaasEnvironment.AWS_LAMBDA);
        }
        if (System.getenv("FUNCTIONS_WORKER_RUNTIME") != null) {
            result.add(FaasEnvironment.AZURE_FUNC);
        }
        if (System.getenv("K_SERVICE") != null || System.getenv("FUNCTION_NAME") != null) {
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
                return System.getenv("AWS_REGION");
            case GCP_FUNC:
                return System.getenv("FUNCTION_REGION");
            case VERCEL:
                return System.getenv("VERCEL_REGION");
            default:
                return null;
        }
    }

    @Nullable
    private static Integer getEnvInteger(final String name) {
        try {
            String value = System.getenv(name);
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
