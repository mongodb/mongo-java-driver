/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package com.mongodb;

/**
 * This class exists only so that on Java 5 the driver can create instances of a standard MBean,
 * therefore keeping compatibility with the JMX implementation in the Java 5 JMX class libraries.
 *
 * @deprecated This class will be removed in 3.x versions of the driver,
 *             so please remove it from your compile time dependencies.
 */
@Deprecated
class Java5MongoConnectionPool extends DBPortPool implements Java5MongoConnectionPoolMBean {

    Java5MongoConnectionPool(ServerAddress addr, MongoOptions options) {
        super(addr, options);
    }
}
