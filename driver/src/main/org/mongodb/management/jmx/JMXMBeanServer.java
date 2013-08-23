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

package org.mongodb.management.jmx;

import org.mongodb.diagnostics.Loggers;
import org.mongodb.management.MBeanServer;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is NOT part of the public API.  It may change at any time without notification.
 */
public class JMXMBeanServer implements MBeanServer {
    private static Logger logger = Loggers.getLogger("management");

    @Override
    public void registerMBean(final Object mBean, final String mBeanName) {
        try {
            server.registerMBean(mBean, new ObjectName(mBeanName));
        } catch (Exception e) {
            logger.log(Level.WARNING, "Unable to register MBean " + mBeanName, e);
        }
    }

    @Override
    public void unregisterMBean(final String mBeanName) {
        try {
            ObjectName objectName = new ObjectName(mBeanName);
            if (server.isRegistered(objectName)) {
                server.unregisterMBean(objectName);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Unable to unregister MBean " + mBeanName, e);
        }
    }

    private final javax.management.MBeanServer server = ManagementFactory.getPlatformMBeanServer();
}
