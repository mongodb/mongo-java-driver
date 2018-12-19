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

package com.mongodb.internal.connection

import com.mongodb.MongoConfigurationException
import com.mongodb.MongoException
import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ClusterType
import com.mongodb.internal.dns.DnsResolver
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class DefaultDnsSrvRecordMonitorSpecification extends Specification {
    def 'should resolve initial hosts'() {
        given:
        def hostName = 'test1.test.build.10gen.cc'
        def resolvedHostOne = 'localhost.test.build.10gen.cc:27017'
        def resolvedHostTwo = 'localhost.test.build.10gen.cc:27018'
        def expectedResolvedHosts = [resolvedHostOne, resolvedHostTwo]
        def dnsSrvRecordInitializer = new TestDnsSrvRecordInitializer(ClusterType.REPLICA_SET, 1)
        def dnsResolver = Mock(DnsResolver) {
            1 * resolveHostFromSrvRecords(hostName) >> expectedResolvedHosts
        }
        def monitor = new DefaultDnsSrvRecordMonitor(hostName, 1, 10000, dnsSrvRecordInitializer, new ClusterId(), dnsResolver)

        when:
        monitor.start()
        def hostsLists = dnsSrvRecordInitializer.waitForInitializedHosts()

        then:
        hostsLists == [[new ServerAddress(resolvedHostOne), new ServerAddress(resolvedHostTwo)] as Set]

        cleanup:
        monitor.close()
    }

    def 'should discover new resolved hosts'() {
        given:
        def hostName = 'test1.test.build.10gen.cc'
        def resolvedHostOne = 'localhost.test.build.10gen.cc:27017'
        def resolvedHostTwo = 'localhost.test.build.10gen.cc:27018'
        def resolvedHostThree = 'localhost.test.build.10gen.cc:27019'
        def expectedResolvedHostsOne = [resolvedHostOne, resolvedHostTwo]
        def expectedResolvedHostsTwo = [resolvedHostTwo, resolvedHostThree]
        def dnsSrvRecordInitializer = new TestDnsSrvRecordInitializer(ClusterType.SHARDED, 2)
        def dnsResolver = Mock(DnsResolver) {
            _ * resolveHostFromSrvRecords(hostName) >>> [expectedResolvedHostsOne, expectedResolvedHostsTwo]
        }
        def monitor = new DefaultDnsSrvRecordMonitor(hostName, 1, 1, dnsSrvRecordInitializer, new ClusterId(), dnsResolver)

        when:
        monitor.start()
        def hostsLists = dnsSrvRecordInitializer.waitForInitializedHosts()

        then:
        hostsLists == [[new ServerAddress(resolvedHostOne), new ServerAddress(resolvedHostTwo)] as Set,
                       [new ServerAddress(resolvedHostTwo), new ServerAddress(resolvedHostThree)] as Set]

        cleanup:
        monitor.close()
    }

    def 'should initialize listener with exception'() {
        given:
        def hostName = 'test1.test.build.10gen.cc'
        def dnsSrvRecordInitializer = new TestDnsSrvRecordInitializer(ClusterType.UNKNOWN, 1)
        def dnsResolver = Mock(DnsResolver) {
            _ * resolveHostFromSrvRecords(hostName) >> {
                throw initializationException
            }
        }
        def monitor = new DefaultDnsSrvRecordMonitor(hostName, 1, 10000, dnsSrvRecordInitializer, new ClusterId(), dnsResolver)

        when:
        monitor.start()
        def initializationExceptionList = dnsSrvRecordInitializer.waitForInitializedException()
        if (!(initializationException instanceof MongoException)) {
            initializationExceptionList[0] = initializationExceptionList[0].getCause()
        }

        then:
        initializationExceptionList == [initializationException]

        cleanup:
        monitor.close()

        where:
        initializationException << [new MongoConfigurationException('test'), new NullPointerException()]
    }

    def 'should not initialize listener with exception after successful initialization'() {
        given:
        def hostName = 'test1.test.build.10gen.cc'
        def resolvedHostListOne = ['localhost.test.build.10gen.cc:27017']
        def resolvedHostListTwo = ['localhost.test.build.10gen.cc:27018']
        def dnsSrvRecordInitializer = new TestDnsSrvRecordInitializer(ClusterType.SHARDED, 2)
        def dnsResolver = Mock(DnsResolver) {
            _ * resolveHostFromSrvRecords(hostName) >> resolvedHostListOne >> { throw initializationException } >> resolvedHostListTwo
        }
        def monitor = new DefaultDnsSrvRecordMonitor(hostName, 1, 1, dnsSrvRecordInitializer, new ClusterId(), dnsResolver)

        when:
        monitor.start()
        def initializedExceptionList = dnsSrvRecordInitializer.waitForInitializedException()

        then:
        initializedExceptionList == []

        cleanup:
        monitor.close()

        where:
        initializationException << [new MongoConfigurationException('test'), new NullPointerException()]
    }

    // Can't use a mock because we need to coordinate the monitor thread with the test via thread synchronization
    static class TestDnsSrvRecordInitializer implements DnsSrvRecordInitializer {

        ClusterType clusterType
        List<Collection<ServerAddress>> hostsList = []
        List<MongoException> initializationExceptionList = []
        CountDownLatch latch

        TestDnsSrvRecordInitializer(ClusterType clusterType, int expectedInitializations) {
            this.clusterType = clusterType
            latch = new CountDownLatch(expectedInitializations)
        }

        List<Collection<ServerAddress>> waitForInitializedHosts() {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError('Timeout waiting for latch')
            }
            hostsList
        }

        List<MongoException> waitForInitializedException() {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError('Timeout waiting for latch')
            }
            initializationExceptionList
        }

        @Override
        void initialize(final Collection<ServerAddress> hosts) {
            if (latch.count > 0) {
                hostsList.add(hosts)
                latch.countDown()
            }
        }

        @Override
        void initialize(final MongoException initializationException) {
            if (latch.count > 0) {
                initializationExceptionList.add(initializationException)
                latch.countDown()
            }
        }

        @Override
        ClusterType getClusterType() {
            clusterType
        }
    }
}
