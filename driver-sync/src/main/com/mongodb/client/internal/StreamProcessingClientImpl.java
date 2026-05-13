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

package com.mongodb.client.internal;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.client.StreamProcessingClient;
import com.mongodb.client.StreamProcessors;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.binding.ClusterBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.StreamFactoryFactory;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.lang.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@ThreadSafe
public final class StreamProcessingClientImpl implements StreamProcessingClient {
    private static final Logger LOGGER = Loggers.getLogger("client");

    private final Cluster cluster;
    private final StreamFactoryFactory streamFactoryFactory;
    private final OperationExecutor executor;
    private final boolean retryReads;
    private final AtomicBoolean closed = new AtomicBoolean();

    public StreamProcessingClientImpl(final Cluster cluster,
                                      final MongoClientSettings settings,
                                      final MongoDriverInformation mongoDriverInformation,
                                      final StreamFactoryFactory streamFactoryFactory) {
        this.cluster = notNull("cluster", cluster);
        this.streamFactoryFactory = notNull("streamFactoryFactory", streamFactoryFactory);
        notNull("settings", settings);
        this.retryReads = settings.getRetryReads();
        this.executor = new SimpleOperationExecutor(cluster, TimeoutSettings.create(settings), settings.getServerApi());
        LOGGER.info("StreamProcessingClient created with settings " + settings);
    }

    @Override
    public StreamProcessors streamProcessors() {
        return new StreamProcessorsImpl(executor, retryReads);
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            cluster.close();
            streamFactoryFactory.close();
        }
    }

    /**
     * A minimal OperationExecutor for stream processing workspaces.
     * No sessions, CSFLE, or transactions are supported.
     */
    private static final class SimpleOperationExecutor implements OperationExecutor {
        private final Cluster cluster;
        private final TimeoutSettings timeoutSettings;
        @Nullable
        private final com.mongodb.ServerApi serverApi;

        SimpleOperationExecutor(final Cluster cluster, final TimeoutSettings timeoutSettings,
                                @Nullable final com.mongodb.ServerApi serverApi) {
            this.cluster = cluster;
            this.timeoutSettings = timeoutSettings;
            this.serverApi = serverApi;
        }

        @Override
        public <T> T execute(final ReadOperation<T, ?> operation, final ReadPreference readPreference,
                             final ReadConcern readConcern) {
            return execute(operation, readPreference, readConcern, null);
        }

        @Override
        public <T> T execute(final WriteOperation<T> operation, final ReadConcern readConcern) {
            return execute(operation, readConcern, null);
        }

        @Override
        public <T> T execute(final ReadOperation<T, ?> operation, final ReadPreference readPreference,
                             final ReadConcern readConcern, @Nullable final com.mongodb.client.ClientSession session) {
            OperationContext operationContext = OperationContext.simpleOperationContext(timeoutSettings, serverApi);
            ReadBinding binding = new ClusterBinding(cluster, readPreference);
            try {
                return operation.execute(binding, operationContext);
            } finally {
                binding.release();
            }
        }

        @Override
        public <T> T execute(final WriteOperation<T> operation, final ReadConcern readConcern,
                             @Nullable final com.mongodb.client.ClientSession session) {
            OperationContext operationContext = OperationContext.simpleOperationContext(timeoutSettings, serverApi);
            WriteBinding binding = new ClusterBinding(cluster, primary());
            try {
                return operation.execute(binding, operationContext);
            } finally {
                binding.release();
            }
        }

        @Override
        public OperationExecutor withTimeoutSettings(final TimeoutSettings newTimeoutSettings) {
            return new SimpleOperationExecutor(cluster, newTimeoutSettings, serverApi);
        }

        @Override
        public TimeoutSettings getTimeoutSettings() {
            return timeoutSettings;
        }
    }
}
