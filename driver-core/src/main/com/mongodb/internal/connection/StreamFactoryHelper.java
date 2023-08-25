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

import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.connection.NettyTransportSettings;
import com.mongodb.connection.StreamFactoryFactory;
import com.mongodb.connection.TransportSettings;
import com.mongodb.connection.netty.NettyStreamFactoryFactory;
import com.mongodb.lang.Nullable;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@SuppressWarnings("deprecation")
public final class StreamFactoryHelper {
    @Nullable
    public static StreamFactoryFactory getStreamFactoryFactoryFromSettings(final MongoClientSettings settings) {
        StreamFactoryFactory streamFactoryFactory;
        TransportSettings transportSettings = settings.getTransportSettings();
        if (transportSettings != null) {
            if (transportSettings instanceof NettyTransportSettings) {
                streamFactoryFactory =
                        NettyStreamFactoryFactory.builder().applySettings((NettyTransportSettings) transportSettings).build();
            } else {
                throw new MongoClientException("Unsupported transport settings: " + transportSettings.getClass().getName());
            }
        } else {
            streamFactoryFactory = settings.getStreamFactoryFactory();
        }
        return streamFactoryFactory;
    }

    private StreamFactoryHelper() {
    }
}
