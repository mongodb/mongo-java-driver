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



package org.mongodb.connection

import org.mongodb.management.JMXConnectionPoolListener
import spock.lang.Specification
import spock.lang.Subject

import javax.management.ObjectName
import java.lang.management.ManagementFactory

class JMXConnectionPoolListenerSpecification extends Specification {
    private static final String CLUSTER_ID = '1'
    private static final ServerAddress SERVER_ADDRESS = new ServerAddress()

    private final connectionFactory = new TestInternalConnectionFactory()

    private provider

    @Subject
    private final JMXConnectionPoolListener jmxListener = new JMXConnectionPoolListener()

    def cleanup() {
        provider.close()
    }

    def 'statistics should reflect values from the provider'() {
        given:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS, connectionFactory,
                ConnectionPoolSettings.builder().minSize(0).maxSize(5).maxWaitQueueSize(1).build(),
                jmxListener)

        when:
        provider.get()
        provider.get().close()

        then:
        with(jmxListener.getMBean(CLUSTER_ID, SERVER_ADDRESS)) {
            host == SERVER_ADDRESS.host
            port == SERVER_ADDRESS.port
            minSize == 0
            maxSize == 5
            size == 2
            checkedOutCount == 1
            waitQueueSize == 0
        }
    }

    def 'should add MBean'() {
        when:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS, connectionFactory,
                ConnectionPoolSettings.builder().minSize(0).maxSize(5).maxWaitQueueSize(1).build(),
                jmxListener)

        then:
        ManagementFactory.getPlatformMBeanServer().isRegistered(
                new ObjectName(jmxListener.getMBeanObjectName(CLUSTER_ID, SERVER_ADDRESS)))
    }

    def 'should remove MBean'() {
        given:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS, connectionFactory,
                ConnectionPoolSettings.builder().minSize(0).maxSize(5).maxWaitQueueSize(1).build(),
                jmxListener)
        when:
        provider.close()

        then:
        jmxListener.getMBean(CLUSTER_ID, SERVER_ADDRESS) == null
        !ManagementFactory.getPlatformMBeanServer().isRegistered(new ObjectName(jmxListener.getMBeanObjectName(CLUSTER_ID, SERVER_ADDRESS)))
    }
}
