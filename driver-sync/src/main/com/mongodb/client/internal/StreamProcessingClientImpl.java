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
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.client.StreamProcessingClient;
import com.mongodb.client.StreamProcessors;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.StreamFactoryFactory;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@ThreadSafe
public final class StreamProcessingClientImpl implements StreamProcessingClient {
    private static final Logger LOGGER = Loggers.getLogger("client");

    private final Cluster cluster;
    private final StreamFactoryFactory streamFactoryFactory;
    private final AtomicBoolean closed = new AtomicBoolean();

    public StreamProcessingClientImpl(final Cluster cluster,
                                      final MongoClientSettings settings,
                                      final MongoDriverInformation mongoDriverInformation,
                                      final StreamFactoryFactory streamFactoryFactory) {
        this.cluster = notNull("cluster", cluster);
        this.streamFactoryFactory = notNull("streamFactoryFactory", streamFactoryFactory);
        LOGGER.info("StreamProcessingClient created with settings " + notNull("settings", settings));
    }

    @Override
    public StreamProcessors streamProcessors() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            cluster.close();
            streamFactoryFactory.close();
        }
    }
}
