/**
 * Copyright (c) 2008 - 2011 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.util.management;

import com.mongodb.util.management.jmx.JMXMBeanServer;

/**
 * This class is NOT part of the public API.  It may change at any time without notification.
 *
 * This class is used to insulate the rest of the driver from the possibility that JMX is not available,
 * as currently is the case on Android VM
 *
 * @deprecated This class will be removed in 3.x versions of the driver,
 *             so please remove it from your compile time dependencies.
 */
@Deprecated
public class MBeanServerFactory {
    static {
        MBeanServer tmp;
        try {
            tmp = new JMXMBeanServer();
        } catch (Throwable e) {
            tmp =  new NullMBeanServer();
        }

        mBeanServer = tmp;
    }

    public static MBeanServer getMBeanServer() {
        return mBeanServer;
    }

    private static final MBeanServer mBeanServer;
}
