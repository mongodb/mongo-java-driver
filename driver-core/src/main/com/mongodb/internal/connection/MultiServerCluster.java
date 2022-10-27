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

import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;

import static com.mongodb.assertions.Assertions.isTrue;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class MultiServerCluster extends AbstractMultiServerCluster {
    public MultiServerCluster(final ClusterId clusterId, final ClusterSettings settings,
                              final ClusterableServerFactory serverFactory) {
        super(clusterId, settings, serverFactory);
        isTrue("srvHost is null", settings.getSrvHost() == null);
        initialize(settings.getHosts());
    }
}
