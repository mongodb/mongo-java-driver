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

import com.mongodb.MongoCredential;
import com.mongodb.MongoInternalException;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.AsyncRunnable.beginAsync;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public abstract class Authenticator {
    private final MongoCredentialWithCache credential;
    private final ClusterConnectionMode clusterConnectionMode;
    private final ServerApi serverApi;

    Authenticator(@NonNull final MongoCredentialWithCache credential, final ClusterConnectionMode clusterConnectionMode,
            @Nullable final ServerApi serverApi) {
        this.credential = credential;
        this.clusterConnectionMode = notNull("clusterConnectionMode", clusterConnectionMode);
        this.serverApi = serverApi;
    }

    public static boolean shouldAuthenticate(@Nullable final Authenticator authenticator,
            final ConnectionDescription connectionDescription) {
        return authenticator != null && connectionDescription.getServerType() != ServerType.REPLICA_SET_ARBITER;
    }

    @NonNull
    MongoCredentialWithCache getMongoCredentialWithCache() {
        return credential;
    }

    @NonNull
    MongoCredential getMongoCredential() {
        return credential.getCredential();
    }

    ClusterConnectionMode getClusterConnectionMode() {
        return clusterConnectionMode;
    }

    @Nullable
    ServerApi getServerApi() {
        return serverApi;
    }

    @NonNull
    String getUserNameNonNull() {
        String userName = credential.getCredential().getUserName();
        if (userName == null) {
            throw new MongoInternalException("User name can not be null");
        }
        return userName;
    }

    @NonNull
    char[] getPasswordNonNull() {
        char[] password = credential.getCredential().getPassword();
        if (password == null) {
            throw new MongoInternalException("Password can not be null");
        }
        return password;
    }

    @NonNull
    <T> T getNonNullMechanismProperty(final String key, @Nullable final T defaultValue) {
        T mechanismProperty = credential.getCredential().getMechanismProperty(key, defaultValue);
        if (mechanismProperty == null) {
            throw new MongoInternalException("Mechanism property can not be null");
        }
        return mechanismProperty;

    }

    abstract void authenticate(InternalConnection connection, ConnectionDescription connectionDescription,
            OperationContext operationContext);

    abstract void authenticateAsync(InternalConnection connection, ConnectionDescription connectionDescription,
            OperationContext operationContext, SingleResultCallback<Void> callback);

    public void reauthenticate(final InternalConnection connection, final OperationContext operationContext) {
        authenticate(connection, connection.getDescription(), operationContextWithoutSession(operationContext));
    }

    public void reauthenticateAsync(final InternalConnection connection, final OperationContext operationContext,
                                    final SingleResultCallback<Void> callback) {
        beginAsync().thenRun((c) -> {
            authenticateAsync(connection, connection.getDescription(), operationContextWithoutSession(operationContext), c);
        }).finish(callback);
    }

    private static OperationContext operationContextWithoutSession(final OperationContext operationContext) {
        return operationContext.withSessionContext(
                new ReadConcernAwareNoOpSessionContext(operationContext.getSessionContext().getReadConcern()));
    }
}
