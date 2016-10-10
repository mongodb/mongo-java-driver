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

package com.mongodb.connection

import com.mongodb.ServerAddress
import com.mongodb.management.JMXConnectionPoolListener
import spock.lang.Specification
import spock.lang.Subject
import spock.lang.Unroll

import javax.management.ObjectName
import java.lang.management.ManagementFactory

class JMXConnectionPoolListenerSpecification extends Specification {
    private static final ServerId SERVER_ID = new ServerId(new ClusterId(), new ServerAddress('host1', 27018))

    private final connectionFactory = new TestInternalConnectionFactory()

    private provider

    @Subject
    private final JMXConnectionPoolListener jmxListener = new JMXConnectionPoolListener()

    def 'statistics should reflect values from the provider'() {
        given:
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                ConnectionPoolSettings.builder().minSize(0).maxSize(5).maxWaitQueueSize(1).build(),
                jmxListener)

        when:
        provider.get()
        provider.get().close()

        then:
        with(jmxListener.getMBean(SERVER_ID)) {
            host == SERVER_ID.address.host
            port == SERVER_ID.address.port
            minSize == 0
            maxSize == 5
            size == 2
            checkedOutCount == 1
            waitQueueSize == 0
        }

        cleanup:
        provider.close()
    }

    def 'should add MBean'() {
        when:
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                ConnectionPoolSettings.builder().minSize(0).maxSize(5).maxWaitQueueSize(1).build(),
                jmxListener)

        then:
        ManagementFactory.getPlatformMBeanServer().isRegistered(
                new ObjectName(jmxListener.getMBeanObjectName(SERVER_ID)))

        cleanup:
        provider.close()
    }

    def 'should remove MBean'() {
        given:
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                ConnectionPoolSettings.builder().minSize(0).maxSize(5).maxWaitQueueSize(1).build(),
                jmxListener)
        when:
        provider.close()

        then:
        jmxListener.getMBean(SERVER_ID) == null
        !ManagementFactory.getPlatformMBeanServer().isRegistered(new ObjectName(jmxListener.getMBeanObjectName(SERVER_ID)))
    }

    def 'should create a valid ObjectName for hostname'() {
        given:
        String beanName = jmxListener.getMBeanObjectName(SERVER_ID);

        when:
        ObjectName objectName = new ObjectName(beanName)

        then:
        objectName.toString() == "org.mongodb.driver:type=ConnectionPool,clusterId=${SERVER_ID.clusterId.value}," +
        "host=${SERVER_ID.address.host},port=${SERVER_ID.address.port}"
    }

    def 'should create a valid ObjectName for ipv4 addresses'() {
        given:
        def serverId = new ServerId(new ClusterId(), new ServerAddress('127.0.0.1'))
        String beanName = jmxListener.getMBeanObjectName(serverId)

        when:
        ObjectName objectName = new ObjectName(beanName)

        then:
        objectName.toString() == "org.mongodb.driver:type=ConnectionPool,clusterId=${serverId.clusterId.value},host=127.0.0.1,port=27017"
    }

    def 'should create a valid ObjectName for ipv6 address'() {
        given:
        def serverId = new ServerId(new ClusterId(), new ServerAddress('[::1]'))
        String beanName = jmxListener.getMBeanObjectName(serverId)

        when:
        ObjectName objectName = new ObjectName(beanName)

        then:
        objectName.toString() == "org.mongodb.driver:type=ConnectionPool,clusterId=${serverId.clusterId.value},host=\"::1\",port=27017"
    }

    def 'should include the description in the object name if set'() {
        when:
        def serverId = new ServerId(new ClusterId(), new ServerAddress())

        then:
        !jmxListener.getMBeanObjectName(serverId).contains('description')

        when:
        serverId = new ServerId(new ClusterId('my app server'), new ServerAddress())

        then:
        jmxListener.getMBeanObjectName(serverId).contains('description')
    }

    @Unroll
    def 'should quote values containing special characters'() {
        when:
        def clusterId = new ClusterId(clusterIdName, description)
        def serverId = new ServerId(clusterId, new ServerAddress(host))
        def objectName = new ObjectName(jmxListener.getMBeanObjectName(serverId))

        then:
        objectName.toString() == "org.mongodb.driver:type=ConnectionPool,clusterId=${expectedClusterIdName},host=${expectedHost}" +
        ",port=27017,description=${expectedDescription}"

        where:
        clusterIdName | expectedClusterIdName | host         | expectedHost     | description            | expectedDescription
        'cluster Id'  | 'cluster Id'          | 'host name'  | 'host name'      | 'client description'   | 'client description'
        'cluster,Id'  | '"cluster,Id"'        | 'host,name'  | '"host,name"'    | 'client, description'  | '"client, description"'
        'cluster:Id'  | '"cluster:Id"'        | 'hostname'   | 'hostname'       | 'client: description'  | '"client: description"'
        'cluster=Id'  | '"cluster=Id"'        | 'host=name'  | '"host=name"'    | 'client= description'  | '"client= description"'
        'cluster"Id'  | '"cluster\\"Id"'      | 'host"name'  | '"host\\"name"'  | 'client" description'  | '"client\\" description"'
        'cluster*Id'  | '"cluster\\*Id"'      | 'host*name'  | '"host\\*name"'  | 'client* description'  | '"client\\* description"'
        'cluster?Id'  | '"cluster\\?Id"'      | 'host?name'  | '"host\\?name"'  | 'client? description'  | '"client\\? description"'
        'cluster\\Id' | '"cluster\\\\Id"'     | 'host\\name' | '"host\\\\name"' | 'client\\ description' | '"client\\\\ description"'
        'cluster\nId' | '"cluster\\nId"'      | 'host\nname' | '"host\\nname"'  | 'client\n description' | '"client\\n description"'
    }
}
