/*
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
 */

package org.mongodb.util.management.jmx;

import org.mongodb.util.management.JMException;
import org.mongodb.util.management.MBeanServer;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * This class is NOT part of the public API.  It may change at any time without notification.
 */
public class JMXMBeanServer implements MBeanServer {
    @Override
    public boolean isRegistered(final String mBeanName) throws JMException {
        return server.isRegistered(createObjectName(mBeanName));
    }

    @Override
    public void unregisterMBean(final String mBeanName) throws JMException {
        try {
            server.unregisterMBean(createObjectName(mBeanName));
        } catch (InstanceNotFoundException e) {
            throw new JMException(e);
        } catch (MBeanRegistrationException e) {
            throw new JMException(e);
        }
    }

    @Override
    public void registerMBean(final Object mBean, final String mBeanName) throws JMException {
        try {
            server.registerMBean(mBean, createObjectName(mBeanName));
        } catch (InstanceAlreadyExistsException e) {
            throw new JMException(e);
        } catch (MBeanRegistrationException e) {
            throw new JMException(e);
        } catch (NotCompliantMBeanException e) {
            throw new JMException(e);
        }
    }

    private ObjectName createObjectName(final String mBeanName) throws JMException {
        try {
            return new ObjectName(mBeanName);
        } catch (MalformedObjectNameException e) {
            throw new JMException(e);
        }
    }

    private final javax.management.MBeanServer server = ManagementFactory.getPlatformMBeanServer();
}
