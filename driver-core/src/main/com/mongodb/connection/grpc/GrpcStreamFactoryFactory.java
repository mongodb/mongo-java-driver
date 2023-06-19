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

package com.mongodb.connection.grpc;

import com.mongodb.ConnectionString;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.Stream;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.StreamFactoryFactory;
import com.mongodb.internal.connection.grpc.SharingGrpcStreamFactoryFactory;
import com.mongodb.internal.connection.grpc.GrpcStreamFactory;
import io.netty.buffer.ByteBufAllocator;

import java.util.UUID;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A {@link StreamFactoryFactory} for <a href="https://grpc.io/">gRPC</a>-based {@linkplain Stream streams}.
 *
 * @see ConnectionString#isGrpc()
 * @since VAKOTODO
 * @mongodb.server.release VAKOTODO
 */
@ThreadSafe
public final class GrpcStreamFactoryFactory implements StreamFactoryFactory {
    private final UUID clientId;
    private final ByteBufAllocator allocator;

    @Override
    public StreamFactory create(final SocketSettings socketSettings, final SslSettings sslSettings) {
        // Since a user may share the same instance of `GrpcStreamFactoryFactory` between `MongoClient`s,
        // all `StreamFactory`s created by `GrpcStreamFactoryFactory` must be isolated from each other,
        // which is why we pass a new instance of `Channels` here.
        return new GrpcStreamFactory(socketSettings, sslSettings, null, null, clientId, allocator,
                new SharingGrpcStreamFactoryFactory.Channels());
    }

    /**
     * Creates a builder for {@link GrpcStreamFactoryFactory}.
     *
     * @return The created {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for {@link GrpcStreamFactoryFactory}.
     *
     * @since VAKOTODO
     * @mongodb.server.release VAKOTODO
     */
    public static final class Builder {
        private ByteBufAllocator allocator;

        Builder() {
            allocator = ByteBufAllocator.DEFAULT;
        }

        /**
         * Sets the allocator.
         *
         * @param allocator The allocator.
         * @return {@code this}.
         */
        public Builder allocator(final ByteBufAllocator allocator) {
            this.allocator = notNull("allocator", allocator);
            return this;
        }

        /**
         * Creates {@link GrpcStreamFactoryFactory}.
         *
         * @return The created {@link GrpcStreamFactoryFactory}.
         */
        public GrpcStreamFactoryFactory build() {
            return new GrpcStreamFactoryFactory(this);
        }
    }

    private GrpcStreamFactoryFactory(final Builder builder) {
        clientId = UUID.randomUUID();
        this.allocator = builder.allocator;
    }

    @Override
    public String toString() {
        return "GrpcStreamFactoryFactory{"
                + "clientId=" + clientId
                + ", allocator=" + allocator
                + '}';
    }
}
