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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoStalePrimaryException;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.TopologyVersion;
import com.mongodb.lang.Nullable;

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLProtocolException;
import java.security.cert.CertPathBuilderException;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertificateException;
import java.util.Optional;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.internal.connection.ClusterableServer.SHUTDOWN_CODES;
import static com.mongodb.internal.connection.ServerDescriptionHelper.unknownConnectingServerDescription;

/**
 * See the
 * <a href="https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.md">
 * Server Discovery And Monitoring</a> specification.
 */
@ThreadSafe
interface SdamServerDescriptionManager {
    /**
     * Receives candidate {@link ServerDescription} from the monitoring activity.
     *
     * @param candidateDescription A {@link ServerDescription} that may or may not be applied
     *                             {@linkplain TopologyVersionHelper#newer(TopologyVersion, TopologyVersion) depending on}
     *                             its {@link ServerDescription#getTopologyVersion() topology version}.
     */
    void monitorUpdate(ServerDescription candidateDescription);

    /**
     * @param candidateDescription A {@link ServerDescription} that may or may not be applied
     *                             {@linkplain TopologyVersionHelper#newer(TopologyVersion, TopologyVersion) depending on}
     *                             its {@link ServerDescription#getTopologyVersion() topology version}.
     */
    void updateToUnknown(ServerDescription candidateDescription);

    void handleExceptionBeforeHandshake(SdamIssue sdamIssue);

    void handleExceptionAfterHandshake(SdamIssue sdamIssue);

    /**
     * Must be used if and only if there is no {@link InternalConnection} available,
     * e.g., if an exception was encountered when checking out a connection,
     * in which case it must be called before (in the happens-before order) checking out.
     * @see #context(InternalConnection)
     */
    SdamIssue.Context context();

    /**
     * Must be preferred to {@link #context()} if an {@link InternalConnection} is available.
     *
     * @see #context()
     */
    SdamIssue.Context context(InternalConnection connection);

    /**
     * Represents an {@linkplain #exception() exception} potentially related to using either a {@link ConnectionPool}
     * or an {@link InternalConnection} from it, and the {@linkplain Context context} in which it occurred.
     */
    @ThreadSafe
    final class SdamIssue {
        @Nullable
        private final Throwable exception;
        private final Context context;

        private SdamIssue(@Nullable final Throwable exception, final Context context) {
            this.exception = exception;
            this.context = assertNotNull(context);
        }

        static SdamIssue of(final Throwable exception, final Context context) {
            return new SdamIssue(assertNotNull(exception), assertNotNull(context));
        }

        /**
         * @return An exception that caused this {@link SdamIssue}.
         */
        Optional<Throwable> exception() {
            return Optional.ofNullable(exception);
        }

        ServerDescription serverDescription() {
            return unknownConnectingServerDescription(context.serverId(), exception);
        }

        boolean stale(final ConnectionPool connectionPool, final ServerDescription currentServerDescription) {
            return context.stale(connectionPool) || stale(exception, currentServerDescription);
        }

        /**
         * @see #relatedToShutdown()
         */
        boolean relatedToStateChange() {
            return exception instanceof MongoNotPrimaryException || exception instanceof MongoNodeIsRecoveringException;
        }

        boolean relatedToStalePrimary() {
            return exception instanceof MongoStalePrimaryException;
        }

        /**
         * Represents a subset of {@link #relatedToStateChange()}.
         *
         * @see #relatedToStateChange()
         */
        boolean relatedToShutdown() {
            assertTrue(relatedToStateChange()); // if this is violated, then we also may need to change the code that uses this method
            //noinspection ConstantConditions
            if (exception instanceof MongoCommandException) {
                return SHUTDOWN_CODES.contains(((MongoCommandException) exception).getErrorCode());
            }
            return false;
        }

        /**
         * @see #relatedToNetworkNotTimeout()
         */
        boolean relatedToNetworkTimeout() {
            return exception instanceof MongoSocketReadTimeoutException;
        }

        /**
         * @return {@code true} if and only if this {@link SdamIssue} is related to
         * network communications and is not {@link #relatedToNetworkTimeout()}.
         * @see #relatedToNetworkTimeout()
         */
        boolean relatedToNetworkNotTimeout() {
            return exception instanceof MongoSocketException && !relatedToNetworkTimeout();
        }

        boolean relatedToAuth() {
            return exception instanceof MongoSecurityException;
        }

        boolean relatedToWriteConcern() {
            return exception instanceof MongoWriteConcernWithResponseException;
        }

        /**
         * Checks if the exception is related to TLS configuration errors that are NOT due to server overload.
         * These include certificate validation failures, protocol mismatches, etc.
         *
         * @return true if this is a TLS configuration error (not network-related)
         */
        boolean relatedToTlsConfigurationError() {
            if (!(exception instanceof MongoSocketException)) {
                return false;
            }
            Throwable cause = exception.getCause();
            while (cause != null) {
                // Check for various certificate validation and TLS configuration errors
                if (cause instanceof CertificateException
                        || cause instanceof CertPathBuilderException
                        || cause instanceof CertPathValidatorException
                        || cause instanceof SSLPeerUnverifiedException
                        || cause instanceof SSLProtocolException) {
                    return true;  // Certificate/peer validation failure or protocol error
                }

                // Check for SunCertPathBuilderException by class name to avoid compile-time dependency on internal classes
                String className = cause.getClass().getName();
                if (className.equals("sun.security.provider.certpath.SunCertPathBuilderException")) {
                    return true;  // Certificate path building failure
                }

                // SSLHandshakeException can be either network or config, so we check the message
                if (cause instanceof SSLHandshakeException) {
                    String message = cause.getMessage();
                    if (message != null) {
                        String lowerMessage = message.toLowerCase();
                        // These indicate configuration issues, not network issues
                        if (lowerMessage.contains("certificate")
                                || lowerMessage.contains("verify")
                                || lowerMessage.contains("trust")
                                || lowerMessage.contains("hostname")
                                || lowerMessage.contains("protocol")
                                || lowerMessage.contains("cipher")
                                || lowerMessage.contains("handshake_failure")) {
                            return true;
                        }
                    }
                }
                cause = cause.getCause();
            }
            return false;
        }

        private static boolean stale(@Nullable final Throwable t, final ServerDescription currentServerDescription) {
            return TopologyVersionHelper.topologyVersion(t)
                    .map(candidateTopologyVersion -> TopologyVersionHelper.newerOrEqual(
                            currentServerDescription.getTopologyVersion(), candidateTopologyVersion))
                    .orElse(false);
        }

        /**
         * A context in which an {@link SdamIssue} occurred. It is used to determine the freshness of the exception.
         */
        @Immutable
        static final class Context {
            private final ServerId serverId;
            private final int connectionPoolGeneration;
            private final int serverMaxWireVersion;

            Context(final ServerId serverId, final int connectionPoolGeneration, final int serverMaxWireVersion) {
                this.serverId = assertNotNull(serverId);
                this.connectionPoolGeneration = connectionPoolGeneration;
                this.serverMaxWireVersion = serverMaxWireVersion;
            }

            private boolean stale(final ConnectionPool connectionPool) {
                return connectionPoolGeneration < connectionPool.getGeneration();
            }

            private ServerId serverId() {
                return serverId;
            }
        }
    }

}
