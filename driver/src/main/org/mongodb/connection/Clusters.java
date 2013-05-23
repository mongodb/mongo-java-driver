/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection;

import org.mongodb.MongoClientOptions;
import org.mongodb.MongoCredential;

import java.util.List;

public final class Clusters {
    private Clusters() {
    }

    public static Cluster create(final ServerAddress serverAddress, final MongoClientOptions options) {
        return create(serverAddress, null, options);
    }

    public static Cluster create(final ServerAddress serverAddress, final List<MongoCredential> credentialList,
                                 final MongoClientOptions options) {
        return new DefaultSingleServerCluster(serverAddress, credentialList, options, new DefaultClusterableServerFactory());
    }

    public static Cluster create(final List<ServerAddress> seedList, final MongoClientOptions options) {
        return create(seedList, null, options);
    }

    public static Cluster create(final List<ServerAddress> seedList, final List<MongoCredential> credentialList,
                                           final MongoClientOptions options) {
        return new DefaultMultiServerCluster(seedList, credentialList, options, new DefaultClusterableServerFactory());
    }
}

