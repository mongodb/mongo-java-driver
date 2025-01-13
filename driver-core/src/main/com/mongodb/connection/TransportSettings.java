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

package com.mongodb.connection;

import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.Sealed;

/**
 * Transport settings for the driver.
 *
 * @since 4.11
 */
@Sealed
@Immutable
public abstract class TransportSettings {
    /**
     * A builder for {@link NettyTransportSettings}.
     *
     * @return a builder for {@link NettyTransportSettings}
     */
    public static NettyTransportSettings.Builder nettyBuilder() {
        return NettyTransportSettings.builder();
    }

    /**
     * A builder for {@link AsyncTransportSettings}.
     *
     * @return a builder for {@link AsyncTransportSettings}
     * @since 5.2
     */
    public static AsyncTransportSettings.Builder asyncBuilder() {
        return AsyncTransportSettings.builder();
    }
}
