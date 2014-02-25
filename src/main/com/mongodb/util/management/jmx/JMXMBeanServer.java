/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.util.management.jmx;

import com.mongodb.util.management.MBeanServer;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.String.format;

/**
 * This class is NOT part of the public API.  It may change at any time without notification.
 *
 * @deprecated This class will be removed in 3.x versions of the driver, so please remove it from your compile time dependencies.
 */
@Deprecated
public class JMXMBeanServer implements MBeanServer {
    private static final Logger LOGGER = Logger.getLogger("com.mongodb.driver.management");

    @Override
    public boolean isRegistered(String mBeanName) {
        try {
            return server.isRegistered(createObjectName(mBeanName));
        } catch (MalformedObjectNameException e) {
            LOGGER.log(Level.WARNING, "Unable to register MBean " + mBeanName, e);
            return false;
        }
    }

    @Override
    public void unregisterMBean(String mBeanName) {
        try {
            server.unregisterMBean(createObjectName(mBeanName));
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Unable to register MBean " + mBeanName, e);
        }
    }

    @Override
    public void registerMBean(Object mBean, String mBeanName) {
        try {
            server.registerMBean(mBean, createObjectName(mBeanName));
        } catch (InstanceAlreadyExistsException e) {
            LOGGER.log(Level.INFO, format("A JMX MBean with the name '%s' already exists", mBeanName));
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Unable to register MBean " + mBeanName, e);
        }
    }

    private ObjectName createObjectName(String mBeanName) throws MalformedObjectNameException {
        return new ObjectName(mBeanName);
    }

    private final javax.management.MBeanServer server = ManagementFactory.getPlatformMBeanServer();
}
