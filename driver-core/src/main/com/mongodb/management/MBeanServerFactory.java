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

package com.mongodb.management;

/**
 * <p>This class is used to insulate the rest of the driver from the possibility that JMX is not available, as currently is the case on
 * Android VM.</p>
 */
final class MBeanServerFactory {
    private MBeanServerFactory() {
    }

    static {
        MBeanServer tmp;
        try {
            tmp = new JMXMBeanServer();
        } catch (Throwable e) {
            tmp = new NullMBeanServer();
        }

        M_BEAN_SERVER = tmp;
    }

    /**
     * Gets the MBeanServer for registering or unregistering MBeans.  This returns a no-op server if JMX is not available (for example, in
     * Android).
     *
     * @return the MBean server.
     */
    static MBeanServer getMBeanServer() {
        return M_BEAN_SERVER;
    }

    private static final MBeanServer M_BEAN_SERVER;
}
