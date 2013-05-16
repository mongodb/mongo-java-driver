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


// TODO: this is a lousy base class (at least it's not public)
/**
 * Base class for classes that manage connections to mongo instances as background tasks.
 */
final class MonitorDefaults  {

    static final int SLAVE_ACCEPTABLE_LATENCY_MS;
    static final int INET_ADDRESS_CACHE_MS;

    static final int UPDATER_INTERVAL_MS;

    static final int UPDATER_INTERVAL_NO_PRIMARY_MS;
    static final float LATENCY_SMOOTH_FACTOR;
    static final MongoClientOptions CLIENT_OPTIONS_DEFAULTS;

    private MonitorDefaults() {
    }

    static {
        SLAVE_ACCEPTABLE_LATENCY_MS = Integer.parseInt(System.getProperty("com.mongodb.slaveAcceptableLatencyMS", "15"));
        INET_ADDRESS_CACHE_MS = Integer.parseInt(System.getProperty("com.mongodb.inetAddrCacheMS", "300000"));

        UPDATER_INTERVAL_MS = Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalMS", "5000"));
        UPDATER_INTERVAL_NO_PRIMARY_MS = Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalNoMasterMS", "10"));

        LATENCY_SMOOTH_FACTOR = Float.parseFloat(System.getProperty("com.mongodb.latencySmoothFactor", "4"));
    }

    static {
        MongoClientOptions.Builder builder = MongoClientOptions.builder();
        builder.connectTimeout(Integer.parseInt(System.getProperty("com.mongodb.updaterConnectTimeoutMS", "20000")));
        builder.socketTimeout(Integer.parseInt(System.getProperty("com.mongodb.updaterSocketTimeoutMS", "20000")));
        CLIENT_OPTIONS_DEFAULTS = builder.build();
    }
}
